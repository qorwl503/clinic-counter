# -*- coding: utf-8 -*-
# ===== 공통 모듈 (데스크/진료실 프로그램에 동일하게 포함됨) =====
import json
import os
import queue
import secrets
import ssl
import sys
import threading
import time
import concurrent.futures
import urllib.request
import urllib.error
import urllib.parse
import tkinter as tk
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import ttk, messagebox, filedialog, simpledialog

VERSION = "v13"
NAMES = ["단순 문진", "내시경 동의서", "종검 설명", "폐암사후", "공검 내시경 설명", "예방접종 문진", "특검"]
ROOMS = ("1", "2")
FONT = "Malgun Gothic"
BG = "#f7f9fd"
DATA_DIR = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "ClinicCounter13"


def today():
    return date.today().isoformat()


def now_hms():
    return datetime.now().strftime("%H:%M:%S")


def new_id():
    """시간순 정렬되는 고유 ID"""
    return "%013d%s" % (int(time.time() * 1000), secrets.token_hex(3))


def load_json(path, default):
    try:
        obj = json.loads(Path(path).read_text(encoding="utf-8"))
        return obj if isinstance(obj, type(default)) else default
    except (OSError, ValueError):
        return default


def save_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


# ---------- 연결 키 ----------
def make_key(url, code):
    return url.rstrip("/") + "|" + code


def parse_key(key):
    """'https://xxx.firebasedatabase.app|코드' -> (url, code). 잘못되면 ValueError"""
    key = (key or "").strip()
    if "|" not in key:
        raise ValueError("연결 키 형식이 아닙니다. (주소|코드)")
    url, code = key.split("|", 1)
    url, code = check_url(url), code.strip()
    if len(code) < 24 or not code.isalnum():
        raise ValueError("연결 코드가 올바르지 않습니다.")
    return url, code


def check_url(url):
    url = (url or "").strip().rstrip("/")
    p = urllib.parse.urlsplit(url)
    host = p.hostname or ""
    if p.scheme == "https" and (host.endswith(".firebaseio.com") or host.endswith(".firebasedatabase.app")):
        return url
    if p.scheme == "http" and host in ("127.0.0.1", "localhost"):  # 시험용
        return url
    raise ValueError("Firebase Realtime Database 주소가 아닙니다.\n예: https://프로젝트-default-rtdb.asia-southeast1.firebasedatabase.app")


# ---------- Firebase REST ----------
class FirebaseDB:
    def __init__(self, url, code):
        self.url = url.rstrip("/")
        self.code = code
        self.ctx = ssl.create_default_context()

    def full(self, path="", params=None):
        path = path.strip("/")
        p = "clinics/" + self.code + ("/" + path if path else "")
        u = "%s/%s.json" % (self.url, urllib.parse.quote(p))
        if params:
            u += "?" + urllib.parse.urlencode(params)
        return u

    def _req(self, method, path, body=None, params=None, timeout=6):
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.full(path, params), data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self.ctx) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise ConnectionError("접근 거부 (DB 규칙 또는 연결 코드 확인)")
            raise ConnectionError("서버 오류 %s" % e.code)
        except (urllib.error.URLError, OSError) as e:
            raise ConnectionError("인터넷 연결 실패: %s" % getattr(e, "reason", e))
        return json.loads(raw.decode("utf-8")) if raw else None

    def get(self, path, params=None):
        return self._req("GET", path, params=params, timeout=15)

    def put(self, path, value):
        return self._req("PUT", path, value)

    def patch(self, path, value):
        return self._req("PATCH", path, value)

    def delete(self, path):
        return self._req("DELETE", path)


class Stream(threading.Thread):
    """Firebase 실시간 수신(SSE). 받은 이벤트는 queue로 메인 스레드에 전달."""

    def __init__(self, db, path, out, tag):
        super().__init__(daemon=True)
        self.db, self.path, self.out, self.tag = db, path, out, tag
        self.stopped = threading.Event()
        self.resp = None

    def stop(self):
        self.stopped.set()
        try:
            if self.resp is not None:
                self.resp.close()
        except Exception:
            pass

    def run(self):
        delay = 1
        while not self.stopped.is_set():
            try:
                req = urllib.request.Request(self.db.full(self.path),
                                             headers={"Accept": "text/event-stream"})
                self.resp = urllib.request.urlopen(req, timeout=75, context=self.db.ctx)
                delay = 1
                event = None
                while not self.stopped.is_set():
                    line = self.resp.readline()
                    if not line:
                        raise ConnectionError("스트림 종료")
                    line = line.decode("utf-8").rstrip("\r\n")
                    if line.startswith("event:"):
                        event = line[6:].strip()
                    elif line.startswith("data:"):
                        data = line[5:].strip()
                        if event in ("put", "patch"):
                            msg = json.loads(data)
                            self.out.put((self.tag, event, msg.get("path", "/"), msg.get("data")))
                        elif event in ("cancel", "auth_revoked"):
                            self.out.put((self.tag, "error", "접근 거부 (DB 규칙 확인)", None))
                            raise ConnectionError("cancel")
                        else:
                            self.out.put((self.tag, "alive", None, None))
            except Exception as e:
                if self.stopped.is_set():
                    break
                self.out.put((self.tag, "error", str(e), None))
                self.stopped.wait(delay)
                delay = min(delay * 2, 20)
            finally:
                try:
                    if self.resp is not None:
                        self.resp.close()
                except Exception:
                    pass


def mirror_apply(root, event, path, data):
    """로컬 사본(dict)에 put/patch 반영. 새 root 반환."""
    keys = [k for k in path.strip("/").split("/") if k]
    if event == "put":
        if not keys:
            return data if isinstance(data, dict) else {}
        node = root if isinstance(root, dict) else {}
        root = node
        for k in keys[:-1]:
            if not isinstance(node.get(k), dict):
                node[k] = {}
            node = node[k]
        if data is None:
            node.pop(keys[-1], None)
        else:
            node[keys[-1]] = data
        return root
    if event == "patch" and isinstance(data, dict):
        for k, v in data.items():
            root = mirror_apply(root, "put", path.rstrip("/") + "/" + k, v)
    return root


def dig(obj, *keys):
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj


def tickets_today(room_node):
    q = dig(room_node, "queue") or {}
    d = today()
    return sorted(k for k, v in q.items() if isinstance(v, dict) and v.get("d") == d)


def task_counts(day_node, room):
    """오늘(또는 특정일) 업무별 건수 [7]"""
    counts = [0] * len(NAMES)
    for ev in (dig(day_node, "events") or {}).values():
        if isinstance(ev, dict) and ev.get("room") == room:
            t = ev.get("task")
            if isinstance(t, int) and 0 <= t < len(NAMES):
                counts[t] += 1
    return counts


def done_ids(day_node, room):
    d = dig(day_node, "done", room) or {}
    return sorted(k for k, v in d.items() if isinstance(v, dict))


def legacy_apply(cur, event, path, data):
    """이전 버전 오늘 기록 [7] 수신용 (리스트도 dict로 보관)"""
    def to_dict(x):
        if isinstance(x, list):
            return {str(i): v for i, v in enumerate(x) if v is not None}
        return x if isinstance(x, dict) else {}
    if path.strip("/") == "" and event == "put":
        return to_dict(data)
    return mirror_apply(to_dict(cur), event, path, data)


def legacy_counts(cur):
    out = [0] * len(NAMES)
    for k, v in (cur or {}).items():
        if str(k).isdigit() and int(k) < len(NAMES) and isinstance(v, int):
            out[int(k)] = v
    return out


def summary_text(counts):
    parts = ["%s %d" % (NAMES[i], c) for i, c in enumerate(counts) if c]
    return " · ".join(parts) if parts else "업무 기록 없음"


# ---------- 비동기 실행 ----------
class Runner:
    """네트워크 작업을 백그라운드에서 실행하고 결과 콜백은 tk 메인 스레드에서 호출"""

    def __init__(self, root, workers=2):
        self.root = root
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        self.jobs = []
        self.root.after(80, self._poll)

    def submit(self, fn, ok=None, fail=None):
        self.jobs.append((self.pool.submit(fn), ok, fail))

    def _poll(self):
        for job in list(self.jobs):
            fut, ok, fail = job
            if not fut.done():
                continue
            self.jobs.remove(job)
            try:
                res = fut.result()
            except Exception as e:
                if fail:
                    fail(e)
            else:
                if ok:
                    ok(res)
        self.root.after(80, self._poll)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)


# ---------- 알림음 ----------
SOUNDS = {
    "딩동": [(988, 0.25), (784, 0.45)],
    "딩동댕": [(784, 0.22), (988, 0.22), (1175, 0.45)],
    "차임 (높은음)": [(1319, 0.18), (1568, 0.18), (2093, 0.4)],
    "차임 (낮은음)": [(523, 0.25), (659, 0.25), (784, 0.45)],
    "삐삐삐": [(1000, 0.12), (0, 0.08), (1000, 0.12), (0, 0.08), (1000, 0.12)],
    "부드러운 종": [(880, 0.9)],
    "Windows 느낌표": "SystemExclamation",
    "Windows 알림": "SystemAsterisk",
    "Windows 경고": "SystemHand",
}
DEFAULT_SOUNDS = {"1": "딩동", "2": "차임 (높은음)"}


def sound_file(name):
    """음 목록을 wav 파일로 만들어 경로 반환 (한 번만 생성)"""
    import wave, math, struct
    notes = SOUNDS.get(name)
    if not isinstance(notes, list):
        return None
    import hashlib
    path = DATA_DIR / "sounds" / ("%s.wav" % hashlib.md5(name.encode("utf-8")).hexdigest()[:10])
    if path.exists():
        return path
    rate, frames = 22050, bytearray()
    for freq, dur in notes:
        n = int(rate * dur)
        for k in range(n):
            if freq:
                env = min(1.0, k / (rate * 0.01)) * math.exp(-3.0 * k / n)
                v = env * (0.6 * math.sin(2 * math.pi * freq * k / rate) +
                           0.2 * math.sin(4 * math.pi * freq * k / rate))
            else:
                v = 0.0
            frames += struct.pack("<h", int(max(-1, min(1, v)) * 30000))
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(bytes(frames))
    return path


def play_sound(name):
    if sys.platform == "win32":
        try:
            import winsound
            alias = SOUNDS.get(name)
            if isinstance(alias, str):
                winsound.PlaySound(alias, winsound.SND_ALIAS | winsound.SND_ASYNC)
            else:
                winsound.PlaySound(str(sound_file(name)), winsound.SND_FILENAME | winsound.SND_ASYNC)
            return
        except Exception:
            pass
    try:
        tk._default_root.bell()
    except Exception:
        pass


def play_alert():
    if sys.platform == "win32":
        try:
            import winsound
            winsound.PlaySound("SystemExclamation", winsound.SND_ALIAS | winsound.SND_ASYNC)
            return
        except Exception:
            pass
    try:
        tk._default_root.bell()
    except Exception:
        pass


def ask_key(parent, title, current=""):
    """연결 키 입력창. 붙여넣기 쉬운 넓은 입력칸."""
    win = tk.Toplevel(parent)
    win.title(title)
    win.attributes("-topmost", True)
    win.resizable(False, False)
    tk.Label(win, text="데스크 프로그램의 '연결 키'를 붙여넣으세요 (Ctrl+V)",
             font=(FONT, 10)).pack(padx=14, pady=(14, 6), anchor="w")
    var = tk.StringVar(value=current)
    ent = tk.Entry(win, textvariable=var, width=70, font=(FONT, 9))
    ent.pack(padx=14, fill="x")
    ent.focus_set()
    result = {"v": None}

    def ok():
        try:
            parse_key(var.get())
        except ValueError as e:
            messagebox.showerror("연결 키 오류", str(e), parent=win)
            return
        result["v"] = var.get().strip()
        win.destroy()

    row = tk.Frame(win)
    row.pack(pady=12)
    tk.Button(row, text="확인", width=10, command=ok).pack(side="left", padx=4)
    tk.Button(row, text="취소", width=10, command=win.destroy).pack(side="left", padx=4)
    win.bind("<Return>", lambda e: ok())
    win.grab_set()
    parent.wait_window(win)
    return result["v"]


# ---------- 월별 통계 창 ----------
class StatsWindow:
    def __init__(self, parent, db, runner, rooms, topmost=True):
        self.db, self.runner, self.rooms = db, runner, rooms
        self.month = date.today().replace(day=1)
        self.data = None
        self.win = tk.Toplevel(parent)
        self.win.title("월별 진료 기록")
        self.win.geometry("780x680")
        self.win.attributes("-topmost", topmost)
        bar = tk.Frame(self.win, padx=10, pady=8)
        bar.pack(fill="x")
        tk.Button(bar, text="◀ 이전", command=lambda: self.move(-1)).pack(side="left")
        self.title = tk.Label(bar, font=(FONT, 14, "bold"))
        self.title.pack(side="left", expand=True)
        tk.Button(bar, text="다음 ▶", command=lambda: self.move(1)).pack(side="right")
        tk.Button(bar, text="이번 달", command=self.this_month).pack(side="right", padx=6)
        self.cal = tk.Frame(self.win, padx=10)
        self.cal.pack(fill="both", expand=True)
        self.selected = None
        self.detail = tk.Label(self.win, justify="left", anchor="w", font=(FONT, 9),
                               fg="#344765", wraplength=730)
        self.detail.pack(fill="x", padx=10, pady=(6, 0))
        self.summary = tk.Label(self.win, justify="left", anchor="w", font=(FONT, 10),
                                fg="#18263b", wraplength=730)
        self.summary.pack(fill="x", padx=10, pady=6)
        foot = tk.Frame(self.win, padx=10, pady=8)
        foot.pack(fill="x")
        tk.Button(foot, text="일별 요약 CSV", command=self.export_daily).pack(side="left")
        tk.Button(foot, text="전체 기록 CSV", command=self.export_events).pack(side="left", padx=8)
        self.load()

    def move(self, off):
        m = self.month.month + off
        y = self.month.year + (m - 1) // 12
        self.month = date(y, (m - 1) % 12 + 1, 1)
        self.load()

    def this_month(self):
        self.month = date.today().replace(day=1)
        self.load()

    def load(self):
        ym = self.month.strftime("%Y-%m")
        self.title.config(text="%d년 %d월" % (self.month.year, self.month.month))
        self.summary.config(text="불러오는 중…")
        self.detail.config(text="")
        self.days = {}
        self.render_calendar()

        def fetch():
            ev = self.db.get("days", {"orderBy": '"$key"', "startAt": '"%s-01"' % ym,
                                      "endAt": '"%s-31"' % ym}) or {}
            legacy = {}
            for r in self.rooms:
                legacy[r] = self.db.get("legacy/" + r) or {}
            return ym, ev, legacy

        self.runner.submit(fetch, self.show, lambda e: self.summary.config(text="불러오기 실패: %s" % e))

    def show(self, res):
        ym, ev, legacy = res
        if ym != self.month.strftime("%Y-%m") or not self.win.winfo_exists():
            return
        days = {}
        hours = {r: [0] * 24 for r in self.rooms}
        blank = lambda: {x: [0, [0] * len(NAMES), False] for x in self.rooms}
        for day, node in (ev or {}).items():
            if not isinstance(node, dict):
                continue
            d = days.setdefault(day, blank())
            for r in self.rooms:
                d[r][1] = task_counts(node, r)
                done = dig(node, "done", r) or {}
                d[r][0] = len(done) + sum(d[r][1])
                times = [v for v in done.values() if isinstance(v, dict)]
                times += [e for e in (dig(node, "events") or {}).values()
                          if isinstance(e, dict) and e.get("room") == r]
                for v in times:
                    try:
                        hours[r][int(str(v.get("time", "0"))[:2])] += 1
                    except (ValueError, AttributeError):
                        pass
        for r in self.rooms:
            for day, counts in (legacy.get(r) or {}).items():
                if not day.startswith(ym):
                    continue
                if isinstance(counts, dict):
                    counts = [counts.get(str(i), 0) for i in range(len(NAMES))]
                if not isinstance(counts, list):
                    continue
                d = days.setdefault(day, blank())
                for i, c in enumerate(counts[:len(NAMES)]):
                    if isinstance(c, int):
                        d[r][1][i] += c
                        d[r][0] += c   # 이전 버전: 업무 1건 = 진료 완료 1명
                d[r][2] = True
        self.days = days
        totals = {r: [0, [0] * len(NAMES)] for r in self.rooms}
        any_legacy = False
        for day in days:
            for r in self.rooms:
                p, t, lg = days[day][r]
                any_legacy = any_legacy or lg
                totals[r][0] += p
                totals[r][1] = [a + b for a, b in zip(totals[r][1], t)]
        self.render_calendar()
        lines = []
        for r in self.rooms:
            p, t = totals[r]
            lines.append("진료실 %s — 진료 완료 %d명, 업무 %d건  |  %s" % (r, p, sum(t), summary_text(t)))
            busy = ["%d시 %d" % (h, n) for h, n in enumerate(hours[r]) if n]
            if busy:
                lines.append("    시간대별 진료 완료: " + " · ".join(busy))
        if any_legacy:
            lines.append("* 이전 버전(v12) 기록 포함 — 업무 건수를 진료 완료 인원으로 함께 계산했습니다.")
        if not days:
            lines = ["이 달 기록이 없습니다."]
        self.summary.config(text="\n".join(lines))
        self.totals = totals

    def render_calendar(self):
        import calendar
        for w in self.cal.winfo_children():
            w.destroy()
        y, m = self.month.year, self.month.month
        for col, wd in enumerate("월화수목금토일"):
            tk.Label(self.cal, text=wd, font=(FONT, 10, "bold"),
                     fg="#b34545" if col == 6 else ("#2a5db0" if col == 5 else "#46566c")).grid(
                row=0, column=col, sticky="nsew")
            self.cal.grid_columnconfigure(col, weight=1, uniform="d")
        today_s = today()
        for wi, week in enumerate(calendar.monthcalendar(y, m), 1):
            self.cal.grid_rowconfigure(wi, weight=1, uniform="w")
            for col, d in enumerate(week):
                if not d:
                    continue
                day = "%04d-%02d-%02d" % (y, m, d)
                info = self.days.get(day)
                lines = [str(d)]
                if info:
                    if len(self.rooms) > 1:
                        for r in self.rooms:
                            p = info[r][0]
                            lines.append("%s실 %d명%s" % (r, p, "*" if info[r][2] else ""))
                    else:
                        r = self.rooms[0]
                        lines.append("%d명%s" % (info[r][0], "*" if info[r][2] else ""))
                bg = "#fff3d6" if day == self.selected else ("#e2efff" if day == today_s else "#f7f9fd")
                tk.Button(self.cal, text="\n".join(lines), justify="left", anchor="nw",
                          font=(FONT, 9), bg=bg, relief="groove", bd=1,
                          fg="#18263b" if info else "#9aa6b8",
                          command=lambda day=day: self.show_day(day)).grid(
                    row=wi, column=col, sticky="nsew", padx=1, pady=1)

    def show_day(self, day):
        self.selected = day
        self.render_calendar()
        info = self.days.get(day)
        if not info:
            self.detail.config(text="%s 기록 없음" % day)
            return
        lines = []
        for r in self.rooms:
            p, t, lg = info[r]
            lines.append("%s 진료실 %s: 진료 완료 %d명 · %s%s" % (day, r, p, summary_text(t),
                                                           "  (이전 버전 기록 포함)" if lg else ""))
        self.detail.config(text="\n".join(lines))

    def export_daily(self):
        if not getattr(self, "days", None):
            return
        import csv
        target = filedialog.asksaveasfilename(parent=self.win, defaultextension=".csv",
                                              initialfile="진료기록_%s.csv" % self.month.strftime("%Y-%m"),
                                              filetypes=[("CSV", "*.csv")])
        if not target:
            return
        with open(target, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            head = ["날짜"]
            for r in self.rooms:
                head += ["진료실%s 진료완료" % r] + ["진료실%s %s" % (r, n) for n in NAMES]
            w.writerow(head)
            for day in sorted(self.days):
                row = [day]
                for r in self.rooms:
                    p, t, _ = self.days[day][r]
                    row += [p] + t
                w.writerow(row)
        messagebox.showinfo("저장 완료", "CSV 파일을 저장했습니다.", parent=self.win)

    def export_events(self):
        import csv
        target = filedialog.asksaveasfilename(parent=self.win, defaultextension=".csv",
                                              initialfile="전체기록_%s.csv" % self.month.strftime("%Y-%m"),
                                              filetypes=[("CSV", "*.csv")])
        if not target:
            return
        ym = self.month.strftime("%Y-%m")

        def fetch():
            return self.db.get("days", {"orderBy": '"$key"', "startAt": '"%s-01"' % ym,
                                        "endAt": '"%s-31"' % ym}) or {}

        def write(days):
            rows = []
            for day, node in days.items():
                for e in (dig(node, "events") or {}).values():
                    if isinstance(e, dict) and e.get("room") in self.rooms:
                        t = e.get("task")
                        name = NAMES[t] if isinstance(t, int) and 0 <= t < len(NAMES) else "?"
                        rows.append([day, e.get("time", ""), e.get("room"), "업무", name])
                for r in self.rooms:
                    for v in (dig(node, "done", r) or {}).values():
                        if isinstance(v, dict):
                            rows.append([day, v.get("time", ""), r, "진료 완료", ""])
            with open(target, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["날짜", "시각", "진료실", "구분", "업무"])
                for row in sorted(rows):
                    w.writerow(row)
            messagebox.showinfo("저장 완료", "CSV 파일을 저장했습니다.", parent=self.win)

        self.runner.submit(fetch, write, lambda e: messagebox.showerror("실패", str(e), parent=self.win))
class TaskDetail:
    """업무 하나의 상세: 오늘/이번 달/올해 건수, 마지막 기록, 오늘 시각 목록"""

    def __init__(self, parent, db, runner, room, task, topmost=True):
        self.win = tk.Toplevel(parent)
        self.win.title("진료실 %s · %s" % (room, NAMES[task]))
        self.win.geometry("380x440")
        self.win.attributes("-topmost", topmost)
        tk.Label(self.win, text="진료실 %s · %s" % (room, NAMES[task]), font=(FONT, 13, "bold")).pack(
            anchor="w", padx=14, pady=(12, 4))
        self.info = tk.Label(self.win, text="불러오는 중…", justify="left", anchor="w", font=(FONT, 11))
        self.info.pack(fill="x", padx=14)
        tk.Label(self.win, text="오늘 기록 시각", font=(FONT, 9, "bold"), fg="#46566c").pack(
            anchor="w", padx=14, pady=(10, 2))
        self.lb = tk.Listbox(self.win, font=(FONT, 10), height=10)
        self.lb.pack(fill="both", expand=True, padx=14, pady=(0, 12))
        y = date.today().year

        def fetch():
            days = db.get("days", {"orderBy": '"$key"', "startAt": '"%d-01-01"' % y,
                                   "endAt": '"%d-12-31"' % y}) or {}
            legacy = db.get("legacy/" + room) or {}
            return days, legacy

        def show(res):
            if not self.win.winfo_exists():
                return
            days, legacy = res
            d0, m0, y0 = today(), today()[:7], str(y)
            cnt = {"d": 0, "m": 0, "y": 0}
            last, times = None, []
            for day, node in days.items():
                for e in (dig(node, "events") or {}).values():
                    if isinstance(e, dict) and e.get("room") == room and e.get("task") == task:
                        stamp = (day, e.get("time", ""))
                        last = max(last, stamp) if last else stamp
                        cnt["y"] += 1
                        cnt["m"] += day.startswith(m0)
                        if day == d0:
                            cnt["d"] += 1
                            times.append(e.get("time", ""))
            for day, v in legacy.items():
                if not day.startswith(y0):
                    continue
                c = legacy_counts(legacy_apply({}, "put", "/", v))[task]
                if c:
                    cnt["y"] += c
                    cnt["m"] += c if day.startswith(m0) else 0
                    cnt["d"] += c if day == d0 else 0
                    if not last or day > last[0]:
                        last = (day, "")
            if last:
                wd = "월화수목금토일"[date.fromisoformat(last[0]).weekday()]
                last_txt = "%s (%s) %s" % (last[0], wd, last[1][:5])
            else:
                last_txt = "기록 없음"
            self.info.config(text="오늘  %d명\n이번 달  %d명\n올해  %d명\n마지막  %s"
                                  % (cnt["d"], cnt["m"], cnt["y"], last_txt))
            for t in sorted(times, reverse=True):
                self.lb.insert(tk.END, t)
            if not times:
                self.lb.insert(tk.END, "오늘 기록 없음")

        runner.submit(fetch, show, lambda e: self.info.config(text="불러오기 실패: %s" % e))
# ===== 공통 모듈 끝 =====

# ===== 진료실 프로그램 =====
CONFIG_FILE = DATA_DIR / "doctor.json"
OUTBOX_FILE = DATA_DIR / "doctor_outbox.json"
OLD_COUNTS = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "ClinicCounter" / "counts.json"
COLORS = ["#286bd5", "#0f8b7c", "#9557bd", "#c77923", "#b34d79", "#527a38", "#6576a3"]
MODS = {"없음": 0, "Ctrl": 2, "Alt": 1, "Shift": 4, "Ctrl+Alt": 3, "Ctrl+Shift": 6, "Alt+Shift": 5}
KEYS = ["없음"] + ["F%d" % i for i in range(1, 13)]
ACTIONS = [("task%d" % i, NAMES[i]) for i in range(len(NAMES))] + \
          [("call", "다음 환자 호출")]
DEFAULT_KEYS = {"task0": "F6", "task1": "F7", "task2": "F8", "task3": "F9", "task4": "F10",
                "task5": "F11", "task6": "없음", "call": "F12"}


def find_old_counts():
    """이전 버전 counts.json 위치 찾기 (Microsoft Store 파이썬은 별도 폴더에 저장됨)"""
    local = Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
    found = []
    cands = [OLD_COUNTS, Path.home() / "ClinicCounter" / "counts.json"]
    try:
        cands += list((local / "Packages").glob("PythonSoftwareFoundation.Python*/LocalCache/Local/ClinicCounter/counts.json"))
    except OSError:
        pass
    for p in cands:
        try:
            if p.is_file() and p not in found:
                found.append(p)
        except OSError:
            pass
    return found


class HotkeyThread(threading.Thread):
    """Windows 전역 단축키. 별도 스레드의 메시지 루프에서 받아 queue로 전달."""
    WM_HOTKEY, WM_QUIT = 0x0312, 0x0012

    def __init__(self, bindings, out):
        super().__init__(daemon=True)
        self.bindings, self.out = bindings, out
        self.tid = None
        self.ready = threading.Event()

    def run(self):
        import ctypes
        from ctypes import wintypes
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32")
        msg = wintypes.MSG()
        user32.PeekMessageW(ctypes.byref(msg), None, 0, 0, 0)
        self.tid = kernel32.GetCurrentThreadId()
        failed, ok = [], []
        for hid, (action, mods, vk, label) in enumerate(self.bindings, start=1):
            if user32.RegisterHotKey(None, hid, mods | 0x4000, vk):
                ok.append(hid)
            else:
                failed.append(label)
        self.out.put(("failed", failed))
        self.ready.set()
        while user32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
            if msg.message == self.WM_HOTKEY and 1 <= msg.wParam <= len(self.bindings):
                self.out.put(("key", self.bindings[msg.wParam - 1][0]))
        for hid in ok:
            user32.UnregisterHotKey(None, hid)

    def stop(self):
        if sys.platform == "win32" and self.ready.wait(2) and self.tid:
            import ctypes
            ctypes.WinDLL("user32").PostThreadMessageW(self.tid, self.WM_QUIT, 0, 0)


class DoctorApp:
    def __init__(self):
        self.cfg = load_json(CONFIG_FILE, {})
        self.cfg.setdefault("room", "1")
        self.cfg.setdefault("mod", "없음")
        keys = dict(DEFAULT_KEYS)
        keys.update(self.cfg.get("keys") or {})
        self.cfg["keys"] = keys
        self.outbox = load_json(OUTBOX_FILE, [])
        self.root = tk.Tk()
        self.root.title("진료실 카운터 " + VERSION)
        self.root.geometry(self.cfg.get("geometry") or "430x700+1040+60")
        self.root.minsize(410, 640)
        self.root.configure(bg=BG)
        self.top_var = tk.BooleanVar(master=self.root, value=self.cfg.get("topmost", True))
        self.root.attributes("-topmost", self.top_var.get())
        self.runner = Runner(self.root, workers=2)
        self.sender = Runner(self.root, workers=1)   # 순서 보장 전송
        self.inbox = queue.Queue()
        self.hk_queue = queue.Queue()
        self.hotkeys = None
        self.db = None
        self.streams = []
        self.room_node = {}
        self.events = {}
        self.legacy = {}
        self.events_day = None
        self.online = False
        self.sending = False
        self.flash_job = None
        self.w = {}
        self.build()
        self.connect()
        self.start_hotkeys()
        self.root.after(100, self.pump)
        self.root.after(1500, self.flush)
        self.root.after(15000, self.tick)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

    @property
    def room(self):
        return self.cfg["room"]

    # ---------- 연결 ----------
    def connect(self):
        for s in self.streams:
            s.stop()
        self.streams, self.room_node, self.events, self.online, self.legacy = [], {}, {}, False, {}
        key = self.cfg.get("key")
        if not key:
            self.db = None
            self.set_status("연결 설정 필요 — ⚙ 설정", "#a44b4b")
            self.render()
            return
        try:
            url, code = parse_key(key)
        except ValueError as e:
            self.db = None
            self.set_status(str(e), "#a44b4b")
            return
        self.db = FirebaseDB(url, code)
        self.set_status("연결 중…", "#8a6d1d")
        self.start_streams()
        self.render()

    def start_streams(self):
        for s in self.streams:
            s.stop()
        self.events_day = today()
        self.events = {}
        self.legacy = {}
        self.streams = [Stream(self.db, "live/rooms/" + self.room, self.inbox, "room"),
                        Stream(self.db, "days/" + self.events_day, self.inbox, "events"),
                        Stream(self.db, "legacy/%s/%s" % (self.room, self.events_day), self.inbox, "legacy")]
        for s in self.streams:
            s.start()

    def pump(self):
        changed = False
        try:
            while True:
                tag, ev, path, data = self.inbox.get_nowait()
                if ev == "error":
                    self.online = False
                    self.set_status("● 오프라인 — 재연결 중", "#a24b4b")
                    continue
                if not self.online:
                    self.online = True
                    self.set_status("● 연결됨", "#24824d")
                    self.root.after(200, self.flush)
                if ev == "alive":
                    continue
                if tag == "legacy":
                    self.legacy = legacy_apply(self.legacy, ev, path, data)
                elif tag == "room":
                    self.room_node = mirror_apply(self.room_node, ev, path, data)
                else:
                    self.events = mirror_apply(self.events, ev, path, data)
                changed = True
        except queue.Empty:
            pass
        try:
            while True:
                kind, val = self.hk_queue.get_nowait()
                if kind == "failed" and val:
                    messagebox.showwarning(
                        "단축키", "다른 프로그램이 이미 쓰고 있어 등록하지 못한 단축키:\n" + ", ".join(val) +
                        "\n\n⚙ 설정에서 다른 키나 조합키(Ctrl 등)로 바꿔주세요.", parent=self.root)
                elif kind == "key":
                    self.on_action(val)
        except queue.Empty:
            pass
        if changed:
            self.render()
        self.root.after(100, self.pump)

    def set_status(self, text, color):
        self.w["net"].config(text=text, fg=color)

    # ---------- 화면 ----------
    def build(self):
        o = tk.Frame(self.root, bg=BG, padx=10, pady=8)
        o.pack(fill="both", expand=True)
        top = tk.Frame(o, bg=BG)
        top.pack(fill="x")
        self.w["title"] = tk.Label(top, bg=BG, fg="#18263b", font=(FONT, 14, "bold"))
        self.w["title"].pack(side="left")
        tk.Button(top, text="⚙", relief="flat", bg="#e8eef9", command=self.settings).pack(side="right")
        tk.Button(top, text="📅 기록", relief="flat", bg="#e8eef9", command=self.stats).pack(side="right", padx=5)
        self.w["net"] = tk.Label(o, bg=BG, anchor="w", font=(FONT, 9))
        self.w["net"].pack(fill="x")

        board = tk.Frame(o, bg="#eaf2fc", padx=10, pady=8)
        board.pack(fill="x", pady=(5, 5))
        self.w["wait"] = tk.Label(board, bg="#eaf2fc", fg="#17539b", font=(FONT, 20, "bold"))
        self.w["wait"].pack(side="left")
        self.w["done"] = tk.Label(board, bg="#eaf2fc", fg="#24824d", font=(FONT, 13, "bold"))
        self.w["done"].pack(side="right")

        self.w["call"] = tk.Button(o, font=(FONT, 12, "bold"), command=lambda: self.on_action("call"))
        self.w["call"].pack(fill="x", ipady=4)
        self.w["callinfo"] = tk.Label(o, bg=BG, fg="#56647b", font=(FONT, 9), anchor="w")
        self.w["callinfo"].pack(fill="x", pady=(2, 4))

        tk.Label(o, text="업무 + 를 누르면 진료 완료(대기 −1)로 함께 기록됩니다. − 는 되돌리기.",
                 bg=BG, fg="#65738a", font=(FONT, 8), anchor="w").pack(fill="x", pady=(0, 3))
        self.w["rows"] = []
        for i, name in enumerate(NAMES):
            row = tk.Frame(o, bg="white", pady=2)
            row.pack(fill="x", pady=2)
            key = tk.Label(row, bg="white", fg="#7a879b", width=5, font=(FONT, 8))
            key.pack(side="left", padx=(4, 0))
            nl = tk.Label(row, text=name, bg="white", fg="#22324a", width=15, anchor="w",
                          font=(FONT, 10, "underline"), cursor="hand2")
            nl.pack(side="left")
            nl.bind("<Button-1>", lambda e, j=i: self.task_detail(j))
            val = tk.Label(row, text="0", bg="white", fg=COLORS[i], width=4, font=(FONT, 12, "bold"))
            val.pack(side="left", expand=True)
            tk.Button(row, text="−", width=3, relief="flat", bg="#e9edf4",
                      command=lambda j=i: self.unrecord(j)).pack(side="right", padx=(2, 5))
            tk.Button(row, text="+", width=3, relief="flat", bg="#dbeaff",
                      command=lambda j=i: self.record(j)).pack(side="right", padx=2)
            self.w["rows"].append((key, val))

        tk.Label(o, text="오늘 입력 시간 (최신순)", bg=BG, fg="#26374d",
                 font=(FONT, 9, "bold")).pack(anchor="w", pady=(8, 2))
        self.w["list"] = tk.Listbox(o, height=5, font=(FONT, 9), activestyle="none")
        self.w["list"].pack(fill="both", expand=True)
        bottom = tk.Frame(o, bg=BG)
        bottom.pack(fill="x", pady=(5, 0))
        tk.Button(bottom, text="선택 기록 삭제", relief="flat", bg="#f3e3e3",
                  command=self.delete_selected).pack(side="left")
        self.w["pending"] = tk.Label(bottom, bg=BG, fg="#a24b4b", font=(FONT, 9))
        self.w["pending"].pack(side="left", padx=8)
        tk.Checkbutton(bottom, text="항상 위", variable=self.top_var, bg=BG,
                       command=self.toggle_top).pack(side="right")
        self.w["msg"] = tk.Label(o, bg=BG, fg="#bd4429", font=(FONT, 10, "bold"), anchor="w")
        self.w["msg"].pack(fill="x")
        self.render()

    def toggle_top(self):
        self.root.attributes("-topmost", self.top_var.get())
        self.cfg["topmost"] = self.top_var.get()
        save_json(CONFIG_FILE, self.cfg)

    def key_label(self, action):
        k = self.cfg["keys"].get(action, "없음")
        if k == "없음":
            return ""
        m = self.cfg.get("mod", "없음")
        return k if m == "없음" else m + "+" + k

    def waiting(self):
        return len(tickets_today(self.room_node))

    def render(self):
        self.w["title"].config(text="진료실 %s" % self.room)
        counts = [a + b for a, b in zip(task_counts(self.events, self.room), legacy_counts(self.legacy))]
        done = done_ids(self.events, self.room)
        completed = len(done) + sum(counts)
        connected = self.db is not None and (self.online or self.room_node)
        w = self.waiting()
        self.w["wait"].config(text=("대기 %d명" % w) if connected else "대기 —")
        self.w["done"].config(text="진료 완료 %d명" % completed)
        kl = self.key_label("call")
        if connected and w > 0:
            self.w["call"].config(text="📢 다음 환자 호출" + ("  (%s)" % kl if kl else ""),
                                  bg="#cfe3ff", fg="#12345a")
        else:
            self.w["call"].config(text="대기 환자 없음" + ("  (%s)" % kl if kl else ""),
                                  bg="#eceff3", fg="#8a96a8")
        call = self.room_node.get("call") if isinstance(self.room_node, dict) else None
        if isinstance(call, dict) and call.get("d") == today():
            self.w["callinfo"].config(text="마지막 호출 %s" % call.get("time", "")[:5])
        else:
            self.w["callinfo"].config(text="오늘 호출 없음")
        for i, (key, val) in enumerate(self.w["rows"]):
            key.config(text=self.key_label("task%d" % i))
            val.config(text=str(counts[i]))
        items = []
        for eid, e in (dig(self.events, "events") or {}).items():
            if isinstance(e, dict) and e.get("room") == self.room:
                t = e.get("task")
                items.append((eid, "ev", e.get("time", ""),
                              NAMES[t] if isinstance(t, int) and 0 <= t < len(NAMES) else "?"))
        for eid in done:
            items.append((eid, "done", self.events["done"][self.room][eid].get("time", ""), "✔ 진료 완료"))
        items.sort(reverse=True)
        pend = {op["eid"] for op in self.outbox}
        lb = self.w["list"]
        sel = lb.curselection()
        lb.delete(0, tk.END)
        self.listed = items
        for eid, kind, t, label in items:
            lb.insert(tk.END, "%s    %s%s" % (t, label, "   ⏳미전송" if eid in pend else ""))
        if sel and sel[0] < lb.size():
            lb.selection_set(sel[0])
        n = len(self.outbox)
        self.w["pending"].config(text=("미전송 %d건 (연결되면 자동 전송)" % n) if n else "")

    def flash(self, text):
        self.w["msg"].config(text=text)
        if self.flash_job:
            self.root.after_cancel(self.flash_job)
        self.flash_job = self.root.after(3500, lambda: self.w["msg"].config(text=""))

    def tick(self):
        if self.db and self.events_day != today():
            self.start_streams()
            self.render()
        self.root.after(15000, self.tick)

    # ---------- 동작 ----------
    def on_action(self, action):
        if action == "call":
            self.call()
        elif action.startswith("task"):
            self.record(int(action[4:]))

    def need_db(self):
        if self.db is None:
            self.flash("먼저 ⚙ 설정에서 연결하세요.")
            return False
        return True

    def queue_op(self, eid, patch, mirror):
        """mirror: [(path, value)] 로컬 즉시 반영"""
        for path, value in mirror:
            if path.startswith("/rooms/"):
                continue
            if path.startswith("q:"):
                self.room_node = mirror_apply(self.room_node, "put", "/queue/" + path[2:], value)
            else:
                self.events = mirror_apply(self.events, "put", path, value)
        self.outbox.append({"eid": eid, "patch": patch})
        save_json(OUTBOX_FILE, self.outbox)
        self.render()
        self.flush()

    def record(self, task):
        if not self.need_db():
            return
        eid, day = new_id(), today()
        ev = {"room": self.room, "task": task, "time": now_hms()}
        patch, mirror = {}, []
        claimed = {k.rsplit("/", 1)[1] for op in self.outbox for k in op["patch"] if "/queue/" in k}
        ids = [t for t in tickets_today(self.room_node) if t not in claimed]
        if ids:
            tid = ids[0]
            ev["ticket"] = {"id": tid, "v": self.room_node["queue"][tid]}
            patch["live/rooms/%s/queue/%s" % (self.room, tid)] = None
            mirror.append(("q:" + tid, None))
        patch["days/%s/events/%s" % (day, eid)] = ev
        if day == self.events_day:
            mirror.append(("/events/" + eid, ev))
        self.queue_op(eid, patch, mirror)

    def unrecord(self, task):
        mine = sorted((k for k, e in (dig(self.events, "events") or {}).items()
                       if isinstance(e, dict) and e.get("room") == self.room and e.get("task") == task),
                      reverse=True)
        if mine:
            self.remove_item(mine[0], "ev")
        elif legacy_counts(self.legacy)[task] > 0:   # 이전 버전에서 올린 오늘 기록 차감
            n = legacy_counts(self.legacy)[task] - 1
            self.legacy[str(task)] = n
            self.render()
            db, path = self.db, "legacy/%s/%s/%d" % (self.room, self.events_day, task)
            self.runner.submit(lambda: db.put(path, n), fail=lambda e: self.flash("전송 실패: %s" % e))

    def delete_selected(self):
        sel = self.w["list"].curselection()
        if not sel:
            self.flash("삭제할 기록을 목록에서 먼저 선택하세요.")
            return
        eid, kind, t, label = self.listed[sel[0]]
        if messagebox.askyesno("기록 삭제", "%s  %s 기록을 삭제할까요?" % (t, label), parent=self.root):
            self.remove_item(eid, kind)

    def remove_item(self, eid, kind):
        day = self.events_day
        if kind == "ev":
            path, local = "days/%s/events/%s" % (day, eid), "/events/" + eid
            val = dig(self.events, "events", eid) or {}
        else:
            path, local = "days/%s/done/%s/%s" % (day, self.room, eid), "/done/%s/%s" % (self.room, eid)
            val = dig(self.events, "done", self.room, eid) or {}
        ticket = val.get("ticket") if isinstance(val, dict) else None
        busy = self.outbox[0] if self.sending and self.outbox else None
        pending = [op for op in self.outbox if op["eid"] == eid and op is not busy]
        if pending:  # 아직 안 보낸 기록은 그냥 취소
            self.outbox.remove(pending[0])
            mirror = [(local, None)]
            if isinstance(ticket, dict):
                mirror.append(("q:" + ticket["id"], ticket["v"]))
            for p_, v in mirror:
                if p_.startswith("q:"):
                    self.room_node = mirror_apply(self.room_node, "put", "/queue/" + p_[2:], v)
                else:
                    self.events = mirror_apply(self.events, "put", p_, v)
            save_json(OUTBOX_FILE, self.outbox)
            self.render()
            return
        patch, mirror = {path: None}, [(local, None)]
        if isinstance(ticket, dict) and isinstance(ticket.get("v"), dict) and ticket["v"].get("d") == today():
            patch["live/rooms/%s/queue/%s" % (self.room, ticket["id"])] = ticket["v"]
            mirror.append(("q:" + ticket["id"], ticket["v"]))
        self.queue_op("x" + eid, patch, mirror)

    def flush(self):
        if self.sending or not self.outbox or self.db is None:
            return
        self.sending = True
        op = self.outbox[0]
        db = self.db

        def done(_):
            self.sending = False
            if op in self.outbox:
                self.outbox.remove(op)
                save_json(OUTBOX_FILE, self.outbox)
            self.render()
            if self.outbox:
                self.root.after(50, self.flush)

        def failed(e):
            self.sending = False
            self.render()
            if "접근 거부" in str(e):
                self.set_status("● " + str(e), "#a24b4b")
            self.root.after(5000, self.flush)

        self.sender.submit(lambda: db.patch("", op["patch"]), done, failed)

    def call(self):
        if self.db is None:
            self.flash("먼저 ⚙ 설정에서 연결하세요.")
            return
        if not self.online:
            play_alert()
            self.flash("연결이 끊겨 호출할 수 없습니다.")
            return
        if self.waiting() <= 0:
            self.flash("대기 환자가 없어 호출하지 않았습니다.")
            return
        val = {"id": new_id(), "time": now_hms(), "d": today()}
        db, room = self.db, self.room

        def ok(_):
            self.room_node = mirror_apply(self.room_node, "put", "/call", val)
            self.flash("📢 %s 호출 보냄" % val["time"][:5])
            self.render()

        def fail(e):
            play_alert()
            self.flash("호출 실패: %s" % e)

        self.runner.submit(lambda: db.put("live/rooms/%s/call" % room, val), ok, fail)

    # ---------- 단축키 ----------
    def start_hotkeys(self):
        if self.hotkeys:
            self.hotkeys.stop()
            self.hotkeys = None
        if sys.platform != "win32":
            return
        mods = MODS.get(self.cfg.get("mod", "없음"), 0)
        bindings = []
        for action, label in ACTIONS:
            k = self.cfg["keys"].get(action, "없음")
            if k in KEYS and k != "없음":
                vk = 0x70 + int(k[1:]) - 1
                bindings.append((action, mods, vk, "%s(%s)" % (label, self.key_label(action))))
        self.hotkeys = HotkeyThread(bindings, self.hk_queue)
        self.hotkeys.start()

    # ---------- 설정 / 기록 ----------
    def task_detail(self, task):
        if self.db is None:
            self.flash("먼저 ⚙ 설정에서 연결하세요.")
            return
        TaskDetail(self.root, self.db, self.runner, self.room, task, self.top_var.get())

    def stats(self):
        if self.db is None:
            self.flash("먼저 ⚙ 설정에서 연결하세요.")
            return
        StatsWindow(self.root, self.db, self.runner, (self.room,), self.top_var.get())

    def settings(self):
        win = tk.Toplevel(self.root)
        win.title("진료실 설정")
        win.attributes("-topmost", True)
        win.resizable(False, False)
        f = tk.Frame(win, padx=14, pady=12)
        f.pack()
        tk.Label(f, text="이 PC의 진료실", font=(FONT, 10, "bold")).grid(row=0, column=0, sticky="w")
        room = tk.StringVar(value=self.room)
        rr = tk.Frame(f)
        rr.grid(row=0, column=1, sticky="w")
        for r in ROOMS:
            tk.Radiobutton(rr, text="진료실 " + r, variable=room, value=r).pack(side="left")

        tk.Label(f, text="연결 키", font=(FONT, 10, "bold")).grid(row=1, column=0, sticky="w", pady=(10, 0))
        key = tk.StringVar(value=self.cfg.get("key", ""))
        tk.Entry(f, textvariable=key, width=52, font=(FONT, 8)).grid(row=1, column=1, sticky="we", pady=(10, 0))
        tk.Label(f, text="데스크 ⚙설정의 '연결 키 복사' 후 여기에 붙여넣기(Ctrl+V)", fg="#65738a",
                 font=(FONT, 8)).grid(row=2, column=1, sticky="w")

        tk.Label(f, text="단축키 조합", font=(FONT, 10, "bold")).grid(row=3, column=0, sticky="w", pady=(12, 0))
        mod = tk.StringVar(value=self.cfg.get("mod", "없음"))
        ttk.Combobox(f, textvariable=mod, values=list(MODS), state="readonly", width=12).grid(
            row=3, column=1, sticky="w", pady=(12, 0))
        tk.Label(f, text="EMR 단축키와 겹치면 Ctrl 또는 Ctrl+Alt 조합을 선택하세요.", fg="#65738a",
                 font=(FONT, 8)).grid(row=4, column=1, sticky="w")
        kv = {}
        for i, (action, label) in enumerate(ACTIONS):
            tk.Label(f, text=label, font=(FONT, 9)).grid(row=5 + i, column=0, sticky="w", padx=(8, 0))
            v = tk.StringVar(value=self.cfg["keys"].get(action, "없음"))
            kv[action] = v
            ttk.Combobox(f, textvariable=v, values=KEYS, state="readonly", width=8).grid(
                row=5 + i, column=1, sticky="w", pady=1)
        nrow = 5 + len(ACTIONS)

        def migrate():
            if self.db is None:
                messagebox.showinfo("이전 기록", "먼저 연결 키를 저장하세요.", parent=win)
                return
            found = find_old_counts()
            if not found:
                if not messagebox.askyesno(
                        "이전 기록", "이전 기록 파일을 자동으로 찾지 못했습니다.\n"
                        "counts.json 파일(또는 이전 버전의 'JSON 백업' 파일)을 직접 선택할까요?", parent=win):
                    return
                path = filedialog.askopenfilename(parent=win, filetypes=[("JSON", "*.json")])
                if not path:
                    return
                found = [Path(path)]
            rows = {}
            for path in found:
                data = load_json(path, {})
                for k, v in data.items():
                    if isinstance(v, list) and len(k) == 10 and k[4] == "-":
                        v = [int(x) if isinstance(x, int) and x > 0 else 0 for x in v][:len(NAMES)]
                        v += [0] * (len(NAMES) - len(v))
                        if sum(v) >= sum(rows.get(k, [0])):
                            rows[k] = v
            if not rows:
                messagebox.showinfo("이전 기록", "선택한 파일에 날짜별 기록이 없습니다.", parent=win)
                return
            days = sorted(rows)
            total = sum(sum(v) for v in rows.values())
            if not messagebox.askyesno("이전 기록", "이전 기록 %d일치 (%s ~ %s, 총 %d건)를\n진료실 %s 기록으로 올립니다.\n"
                                       "(같은 날짜는 최신 값으로 덮어씀) 계속할까요?"
                                       % (len(days), days[0], days[-1], total, self.room), parent=win):
                return
            db, r = self.db, self.room
            self.runner.submit(lambda: db.patch("legacy/" + r, rows),
                               lambda _: messagebox.showinfo("완료", "이전 기록을 올렸습니다.\n📅 기록에서 확인할 수 있습니다.", parent=win),
                               lambda e: messagebox.showerror("실패", str(e), parent=win))

        tk.Button(f, text="이전 버전(v12) 기록 서버로 올리기", command=migrate).grid(
            row=nrow, column=0, columnspan=2, sticky="w", pady=(12, 0))

        def save():
            k = key.get().strip()
            if k:
                try:
                    parse_key(k)
                except ValueError as e:
                    messagebox.showerror("연결 키 오류", str(e), parent=win)
                    return
            used = [v.get() for v in kv.values() if v.get() != "없음"]
            if len(used) != len(set(used)):
                messagebox.showerror("단축키", "같은 키가 두 번 지정되었습니다.", parent=win)
                return
            reconnect = k != self.cfg.get("key", "") or room.get() != self.room
            self.cfg.update({"key": k, "room": room.get(), "mod": mod.get(),
                             "keys": {a: v.get() for a, v in kv.items()}})
            save_json(CONFIG_FILE, self.cfg)
            win.destroy()
            if reconnect:
                self.connect()
            self.start_hotkeys()
            self.render()

        b = tk.Frame(f)
        b.grid(row=nrow + 1, column=0, columnspan=2, pady=(14, 0))
        tk.Button(b, text="저장", width=10, command=save).pack(side="left", padx=4)
        tk.Button(b, text="취소", width=10, command=win.destroy).pack(side="left", padx=4)

    def close(self):
        if self.outbox and not messagebox.askyesno(
                "미전송 기록", "아직 전송 안 된 기록이 %d건 있습니다.\n종료해도 다음 실행 때 자동 전송됩니다. 종료할까요?"
                % len(self.outbox), parent=self.root):
            return
        if self.hotkeys:
            self.hotkeys.stop()
        for s in self.streams:
            s.stop()
        self.cfg["geometry"] = self.root.geometry()
        save_json(CONFIG_FILE, self.cfg)
        self.runner.close()
        self.sender.close()
        self.root.destroy()


if __name__ == "__main__":
    DoctorApp().root.mainloop()
