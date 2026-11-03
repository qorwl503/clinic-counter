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
        self.win.geometry("760x600")
        self.win.attributes("-topmost", topmost)
        bar = tk.Frame(self.win, padx=10, pady=8)
        bar.pack(fill="x")
        tk.Button(bar, text="◀ 이전", command=lambda: self.move(-1)).pack(side="left")
        self.title = tk.Label(bar, font=(FONT, 14, "bold"))
        self.title.pack(side="left", expand=True)
        tk.Button(bar, text="다음 ▶", command=lambda: self.move(1)).pack(side="right")
        tk.Button(bar, text="이번 달", command=self.this_month).pack(side="right", padx=6)
        cols = ["day"]
        for r in rooms:
            cols += ["p" + r, "t" + r]
        if len(rooms) > 1:
            cols.append("sum")
        self.tree = ttk.Treeview(self.win, columns=cols, show="headings", height=14)
        self.tree.heading("day", text="날짜")
        self.tree.column("day", width=110, anchor="center")
        for r in rooms:
            self.tree.heading("p" + r, text="진료실%s 완료" % r)
            self.tree.heading("t" + r, text="진료실%s 업무" % r)
            self.tree.column("p" + r, width=110, anchor="center")
            self.tree.column("t" + r, width=110, anchor="center")
        if len(rooms) > 1:
            self.tree.heading("sum", text="완료 합계")
            self.tree.column("sum", width=100, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=10)
        self.tree.bind("<<TreeviewSelect>>", self.show_day)
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
        self.tree.delete(*self.tree.get_children())

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
                d[r][0] = len(done)
                for v in done.values():
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
                d[r][2] = True
        self.days = days
        self.rows = []
        totals = {r: [0, [0] * len(NAMES)] for r in self.rooms}
        any_legacy = False
        for day in sorted(days):
            vals = [day[5:].replace("-", "/") + " (%s)" % "월화수목금토일"[date.fromisoformat(day).weekday()]]
            psum = 0
            for r in self.rooms:
                p, t, lg = days[day][r]
                any_legacy = any_legacy or lg
                vals += [p, str(sum(t)) + ("*" if lg else "")]
                psum += p
                totals[r][0] += p
                totals[r][1] = [a + b for a, b in zip(totals[r][1], t)]
            if len(self.rooms) > 1:
                vals.append(psum)
            self.tree.insert("", "end", iid=day, values=vals)
        lines = []
        for r in self.rooms:
            p, t = totals[r]
            lines.append("진료실 %s — 진료 완료 %d명, 업무 %d건  |  %s" % (r, p, sum(t), summary_text(t)))
            busy = ["%d시 %d" % (h, n) for h, n in enumerate(hours[r]) if n]
            if busy:
                lines.append("    시간대별 진료 완료: " + " · ".join(busy))
        if any_legacy:
            lines.append("* 이전 버전(v12) 기록 포함 — 업무 건수만 있고 진료 완료 인원은 없습니다.")
        if not days:
            lines = ["이 달 기록이 없습니다."]
        self.summary.config(text="\n".join(lines))
        self.totals = totals

    def show_day(self, _e=None):
        sel = self.tree.selection()
        if not sel:
            return
        day = sel[0]
        lines = []
        for r in self.rooms:
            p, t, _ = self.days[day][r]
            lines.append("%s 진료실 %s: 진료 완료 %d명 · %s" % (day, r, p, summary_text(t)))
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
# ===== 공통 모듈 끝 =====

# ===== 데스크 프로그램 =====
CONFIG_FILE = DATA_DIR / "desk.json"
ONLINE_SEC = 30


class DeskApp:
    def __init__(self):
        self.cfg = load_json(CONFIG_FILE, {})
        if not self.cfg.get("id"):
            self.cfg["id"] = "d" + secrets.token_hex(6)
            self.cfg.setdefault("name", "데스크")
            save_json(CONFIG_FILE, self.cfg)
        self.root = tk.Tk()
        self.root.title("데스크 · 대기/호출 " + VERSION)
        self.root.geometry(self.cfg.get("geometry") or "540x700+80+60")
        self.root.minsize(500, 640)
        self.root.configure(bg=BG)
        self.top_var = tk.BooleanVar(master=self.root, value=self.cfg.get("topmost", True))
        self.root.attributes("-topmost", self.top_var.get())
        self.runner = Runner(self.root, workers=3)
        self.inbox = queue.Queue()
        self.db = None
        self.streams = []
        self.live = {}
        self.events = {}
        self.events_day = None
        self.online = False
        self.last_call = {r: None for r in ROOMS}
        self.popups = {}
        self.unacked = set()
        self.seen = {}
        self.assign_hold = 0
        self.w = {}
        self.build()
        self.connect()
        self.root.after(100, self.pump)
        self.root.after(1000, self.tick)
        self.root.after(500, self.heartbeat)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

    # ---------- 연결 ----------
    def connect(self):
        for s in self.streams:
            s.stop()
        self.streams = []
        self.live, self.events, self.online = {}, {}, False
        self.last_call = {r: None for r in ROOMS}
        key = self.cfg.get("key")
        if not key:
            self.set_status("연결 설정 필요 — ⚙ 설정을 눌러주세요", "#a44b4b")
            self.render()
            return
        try:
            url, code = parse_key(key)
        except ValueError as e:
            self.set_status(str(e), "#a44b4b")
            return
        self.db = FirebaseDB(url, code)
        self.set_status("연결 중…", "#8a6d1d")
        self.start_streams()

    def start_streams(self):
        for s in self.streams:
            s.stop()
        self.events_day = today()
        self.streams = [Stream(self.db, "live", self.inbox, "live"),
                        Stream(self.db, "days/" + self.events_day, self.inbox, "events")]
        for s in self.streams:
            s.start()

    def pump(self):
        changed = False
        try:
            while True:
                tag, ev, path, data = self.inbox.get_nowait()
                if ev == "error":
                    self.online = False
                    self.set_status("● 연결 끊김 — 재연결 중 (%s)" % path, "#a44b4b")
                    continue
                if not self.online:
                    self.online = True
                    self.set_status("● 실시간 연결됨", "#21804c")
                if ev == "alive":
                    continue
                if tag == "live":
                    self.live = mirror_apply(self.live, ev, path, data)
                else:
                    self.events = mirror_apply(self.events, ev, path, data)
                changed = True
        except queue.Empty:
            pass
        if changed:
            self.check_calls()
            self.render()
        self.root.after(100, self.pump)

    def set_status(self, text, color):
        self.w["status"].config(text=text, fg=color)

    # ---------- 화면 ----------
    def build(self):
        f = tk.Frame(self.root, bg=BG, padx=14, pady=10)
        f.pack(fill="both", expand=True)
        top = tk.Frame(f, bg=BG)
        top.pack(fill="x")
        self.w["title"] = tk.Label(top, text="데스크", bg=BG, font=(FONT, 15, "bold"), fg="#18263b")
        self.w["title"].pack(side="left")
        tk.Button(top, text="⚙ 설정", relief="flat", bg="#e8eef9", command=self.settings).pack(side="right")
        tk.Button(top, text="📊 월별 통계", relief="flat", bg="#e8eef9", command=self.stats).pack(side="right", padx=6)
        self.w["status"] = tk.Label(f, bg=BG, anchor="w", font=(FONT, 9))
        self.w["status"].pack(fill="x", pady=(2, 6))

        mine = tk.Frame(f, bg="#fff8e6", padx=10, pady=7)
        mine.pack(fill="x", pady=(0, 6))
        tk.Label(mine, text="내가 받을 호출:", bg="#fff8e6", font=(FONT, 10, "bold")).pack(side="left")
        self.mode = tk.StringVar(master=self.root, value="")
        for value, label in (("1", "진료실 1"), ("2", "진료실 2"), ("both", "전체 담당")):
            tk.Radiobutton(mine, text=label, variable=self.mode, value=value, bg="#fff8e6",
                           font=(FONT, 10), command=self.set_mode).pack(side="left", padx=4)

        for r in ROOMS:
            box = tk.Frame(f, bg="#eaf2fc", padx=12, pady=9)
            box.pack(fill="x", pady=5)
            head = tk.Frame(box, bg="#eaf2fc")
            head.pack(fill="x")
            tk.Label(head, text="진료실 " + r, bg="#eaf2fc", font=(FONT, 13, "bold")).pack(side="left")
            own = tk.Label(head, bg="#eaf2fc", font=(FONT, 9))
            own.pack(side="right")
            big = tk.Label(box, bg="#eaf2fc", fg="#1d62b2", font=(FONT, 20, "bold"), anchor="w")
            big.pack(fill="x", pady=(2, 0))
            tasks = tk.Label(box, bg="#eaf2fc", fg="#3d4e66", font=(FONT, 9), anchor="w",
                             justify="left", wraplength=480)
            tasks.pack(fill="x")
            call = tk.Label(box, bg="#eaf2fc", fg="#56647b", font=(FONT, 10), anchor="w")
            call.pack(fill="x", pady=(3, 6))
            btns = tk.Frame(box, bg="#eaf2fc")
            btns.pack(fill="x")
            tk.Button(btns, text="＋ 대기 등록", bg="#cfe3ff", width=16, font=(FONT, 12, "bold"),
                      command=lambda r=r: self.add(r)).pack(side="left", padx=(0, 8))
            tk.Button(btns, text="− 등록 취소", bg="#f1f2f5", font=(FONT, 10),
                      command=lambda r=r: self.remove(r)).pack(side="left")
            ack = tk.Button(btns, text="호출 확인", bg="#ffcf9e", font=(FONT, 10, "bold"),
                            command=lambda r=r: self.ack(r))
            self.w[r] = {"box": box, "own": own, "big": big, "tasks": tasks, "call": call,
                         "ack": ack, "head": head, "btns": btns}

        bottom = tk.Frame(f, bg=BG)
        bottom.pack(fill="x", side="bottom")
        tk.Checkbutton(bottom, text="항상 위", variable=self.top_var, bg=BG,
                       command=self.toggle_top).pack(side="right")
        self.w["foot"] = tk.Label(bottom, bg=BG, fg="#65738a", font=(FONT, 8), anchor="w")
        self.w["foot"].pack(side="left")
        self.render()

    def toggle_top(self):
        self.root.attributes("-topmost", self.top_var.get())
        self.cfg["topmost"] = self.top_var.get()
        save_json(CONFIG_FILE, self.cfg)

    def desk_online(self, desk_id):
        if desk_id == self.cfg["id"]:
            return True
        info = dig(self.live, "desks", desk_id)
        if not isinstance(info, dict):
            return False
        seen = info.get("seen")
        now = time.time()
        prev = self.seen.get(desk_id)
        if prev is None or prev[0] != seen:
            self.seen[desk_id] = (seen, now)
            prev = self.seen[desk_id]
        fresh_local = now - prev[1] < ONLINE_SEC
        fresh_clock = isinstance(seen, (int, float)) and abs(now * 1000 - seen) < ONLINE_SEC * 1000
        return fresh_local or fresh_clock

    def owner(self, room):
        """(담당 데스크 id, 이름, 온라인 여부)"""
        oid = dig(self.live, "assign", room) or ""
        if not oid:
            return "", "", False
        name = dig(self.live, "desks", oid, "name") or "다른 데스크"
        return oid, name, self.desk_online(oid)

    def render(self):
        self.w["title"].config(text="%s · 대기/호출" % self.cfg.get("name", "데스크"))
        for r in ROOMS:
            w = self.w[r]
            node = dig(self.live, "rooms", r) or {}
            waiting = len(tickets_today(node))
            patients = len(done_ids(self.events, r))
            counts = task_counts(self.events, r)
            if self.db is None or not self.live and not self.online:
                w["big"].config(text="대기 —")
            else:
                w["big"].config(text="대기 %d명      진료 완료 %d명" % (waiting, patients))
            w["tasks"].config(text=summary_text(counts))
            call = node.get("call") if isinstance(node.get("call"), dict) else None
            if call and call.get("d") == today():
                mins = self.minutes_since(call.get("time", ""))
                txt = "마지막 호출 %s" % call.get("time", "")[:5] + (" (%d분 전)" % mins if mins is not None else "")
            else:
                txt = "오늘 호출 없음"
            oid, oname, on = self.owner(r)
            if oid == self.cfg["id"]:
                w["own"].config(text="담당: 나", fg="#21804c")
            elif oid and on:
                w["own"].config(text="담당: " + oname, fg="#1d62b2")
            else:
                w["own"].config(text="담당 없음 → 모든 데스크에 알림", fg="#bd4429")
            alert = r in self.unacked
            color = "#ffe2c4" if alert else "#eaf2fc"
            for key in ("box", "head", "btns"):
                w[key].config(bg=color)
            for key in ("own", "big", "tasks", "call"):
                w[key].config(bg=color)
            for child in w["head"].winfo_children():
                child.config(bg=color)
            w["call"].config(text=("🔔 호출됨 — 환자 안내 후 '호출 확인'  |  " if alert else "") + txt,
                             fg="#bd4429" if alert else "#56647b")
            if alert:
                w["ack"].pack(side="right")
            else:
                w["ack"].pack_forget()

        if time.time() > self.assign_hold:
            mine = [r for r in ROOMS if dig(self.live, "assign", r) == self.cfg["id"]]
            self.mode.set("both" if len(mine) == 2 else (mine[0] if mine else ""))
        self.w["foot"].config(text="다른 데스크가 같은 진료실을 고르면 그쪽으로 넘어갑니다. 담당이 없으면 모든 데스크에 알림.")

    @staticmethod
    def minutes_since(hms):
        try:
            t = datetime.combine(date.today(), datetime.strptime(hms, "%H:%M:%S").time())
            return max(0, int((datetime.now() - t).total_seconds() // 60))
        except ValueError:
            return None

    def tick(self):
        if self.db and self.events_day != today():
            self.events = {}
            self.unacked.clear()
            self.start_streams()
            self.runner.submit(self.cleanup_old_tickets)
        self.render()
        self.root.after(15000 if not self.unacked else 5000, self.tick)

    def heartbeat(self):
        if self.db:
            body = {"name": self.cfg.get("name", "데스크"), "seen": {".sv": "timestamp"}}
            db, did = self.db, self.cfg["id"]
            self.runner.submit(lambda: db.put("live/desks/" + did, body))
        self.root.after(10000, self.heartbeat)

    def cleanup_old_tickets(self):
        d = today()
        patch = {}
        for r in ROOMS:
            for k, v in (dig(self.live, "rooms", r, "queue") or {}).items():
                if not isinstance(v, dict) or v.get("d") != d:
                    patch["live/rooms/%s/queue/%s" % (r, k)] = None
        if patch:
            self.db.patch("", patch)

    # ---------- 동작 ----------
    def need_db(self):
        if self.db is None:
            messagebox.showinfo("연결 설정", "먼저 ⚙ 설정에서 연결하세요.", parent=self.root)
            return False
        return True

    def fail(self, e):
        play_alert()
        self.set_status("● 전송 실패: %s" % e, "#a44b4b")

    def add(self, r):
        if not self.need_db():
            return
        tid = new_id()
        val = {"t": now_hms(), "d": today()}
        self.live = mirror_apply(self.live, "put", "/rooms/%s/queue/%s" % (r, tid), val)
        self.render()
        db = self.db
        self.runner.submit(lambda: db.put("live/rooms/%s/queue/%s" % (r, tid), val), fail=self.fail)

    def remove(self, r):
        if not self.need_db():
            return
        ids = tickets_today(dig(self.live, "rooms", r))
        if not ids:
            return
        tid = ids[-1]
        self.live = mirror_apply(self.live, "put", "/rooms/%s/queue/%s" % (r, tid), None)
        self.render()
        db = self.db
        self.runner.submit(lambda: db.delete("live/rooms/%s/queue/%s" % (r, tid)), fail=self.fail)

    def set_mode(self):
        """담당 선택: 고른 진료실은 내가 가져오고, 안 고른 진료실은 내 담당이면 해제"""
        if not self.need_db():
            self.mode.set("")
            return
        self.assign_hold = time.time() + 4
        want = ROOMS if self.mode.get() == "both" else (self.mode.get(),)
        patch = {}
        for r in ROOMS:
            if r in want:
                patch["live/assign/" + r] = self.cfg["id"]
            elif dig(self.live, "assign", r) == self.cfg["id"]:
                patch["live/assign/" + r] = None
        for k, v in patch.items():
            self.live = mirror_apply(self.live, "put", k[4:], v)
        self.render()
        db = self.db
        self.runner.submit(lambda: db.patch("", patch), fail=self.fail)

    # ---------- 호출 알림 ----------
    def check_calls(self):
        for r in ROOMS:
            call = dig(self.live, "rooms", r, "call")
            cid = call.get("id") if isinstance(call, dict) else None
            if self.last_call[r] is None:
                self.last_call[r] = cid or ""
                continue
            if cid and cid != self.last_call[r]:
                self.last_call[r] = cid
                if call.get("d") != today():
                    continue
                oid, _, on = self.owner(r)
                if oid == self.cfg["id"]:
                    self.alert(r, call.get("time", ""), False)
                elif not oid or not on:
                    self.alert(r, call.get("time", ""), True)

    def alert(self, r, when, unowned):
        self.unacked.add(r)
        play_alert()
        self.root.after(1500, play_alert)
        old = self.popups.pop(r, None)
        if old is not None:
            try:
                old.destroy()
            except tk.TclError:
                pass
        p = tk.Toplevel(self.root)
        self.popups[r] = p
        p.title("진료실 %s 호출" % r)
        sw = p.winfo_screenwidth()
        x = sw // 2 - 460 if r == "1" else sw // 2 + 20
        p.geometry("440x190+%d+120" % max(0, x))
        p.attributes("-topmost", True)
        p.configure(bg="#fff0df")
        tk.Label(p, text="🔔 진료실 %s" % r, font=(FONT, 22, "bold"), bg="#fff0df",
                 fg="#a83725").pack(pady=(18, 0))
        tk.Label(p, text="다음 환자 모셔주세요", font=(FONT, 15, "bold"), bg="#fff0df",
                 fg="#a83725").pack()
        tk.Label(p, text="호출 %s%s" % (when, "  (담당 데스크 없음)" if unowned else ""),
                 font=(FONT, 10), bg="#fff0df").pack(pady=(4, 6))
        tk.Button(p, text="확인", width=14, font=(FONT, 11, "bold"), bg="#ffcf9e",
                  command=lambda: self.ack(r)).pack()
        p.protocol("WM_DELETE_WINDOW", lambda: self.ack(r))
        self.render()

    def ack(self, r):
        self.unacked.discard(r)
        p = self.popups.pop(r, None)
        if p is not None:
            try:
                p.destroy()
            except tk.TclError:
                pass
        self.render()

    # ---------- 설정 / 통계 ----------
    def stats(self):
        if not self.need_db():
            return
        StatsWindow(self.root, self.db, self.runner, ROOMS, self.top_var.get())

    def settings(self):
        win = tk.Toplevel(self.root)
        win.title("데스크 설정")
        win.attributes("-topmost", True)
        win.resizable(False, False)
        pad = {"padx": 14, "anchor": "w"}
        tk.Label(win, text="이 데스크 이름", font=(FONT, 10, "bold")).pack(pady=(14, 2), **pad)
        name = tk.StringVar(value=self.cfg.get("name", "데스크"))
        tk.Entry(win, textvariable=name, width=24, font=(FONT, 10)).pack(**pad)
        tk.Label(win, text="예: 데스크 1, 데스크 2 (호출 담당 표시에 사용)", fg="#65738a",
                 font=(FONT, 8)).pack(**pad)

        tk.Label(win, text="연결 키", font=(FONT, 10, "bold")).pack(pady=(14, 2), **pad)
        key = tk.StringVar(value=self.cfg.get("key", ""))
        tk.Entry(win, textvariable=key, width=72, font=(FONT, 9)).pack(padx=14, fill="x")
        row = tk.Frame(win)
        row.pack(pady=6, **pad)

        def copy():
            win.clipboard_clear()
            win.clipboard_append(key.get().strip())
            messagebox.showinfo("복사됨", "연결 키를 복사했습니다.\n다른 PC 설정에 붙여넣으세요.", parent=win)

        def create():
            url = simpledialog.askstring(
                "처음 설정", "Firebase Realtime Database 주소를 붙여넣으세요\n"
                "(예: https://xxx-default-rtdb.asia-southeast1.firebasedatabase.app)", parent=win)
            if not url:
                return
            try:
                url = check_url(url)
            except ValueError as e:
                messagebox.showerror("주소 오류", str(e), parent=win)
                return
            if key.get().strip() and not messagebox.askyesno(
                    "새 연결 키", "기존 연결 키를 버리고 새로 만듭니다.\n다른 PC도 모두 새 키로 바꿔야 합니다. 계속할까요?",
                    parent=win):
                return
            key.set(make_key(url, secrets.token_hex(16)))

        tk.Button(row, text="연결 키 복사", command=copy).pack(side="left")
        tk.Button(row, text="처음 설정 (새 연결 키 만들기)", command=create).pack(side="left", padx=6)
        tk.Label(win, text="처음 한 번만 '새 연결 키 만들기'를 하고,\n나머지 PC에는 그 키를 복사해 붙여넣으세요.",
                 fg="#65738a", font=(FONT, 8), justify="left").pack(**pad)

        def save():
            k = key.get().strip()
            if k:
                try:
                    parse_key(k)
                except ValueError as e:
                    messagebox.showerror("연결 키 오류", str(e), parent=win)
                    return
            changed = k != self.cfg.get("key", "")
            self.cfg["name"] = name.get().strip() or "데스크"
            self.cfg["key"] = k
            save_json(CONFIG_FILE, self.cfg)
            win.destroy()
            if changed:
                self.connect()
            self.render()

        b = tk.Frame(win)
        b.pack(pady=14)
        tk.Button(b, text="저장", width=10, command=save).pack(side="left", padx=4)
        tk.Button(b, text="취소", width=10, command=win.destroy).pack(side="left", padx=4)

    def close(self):
        if self.db:
            try:
                db, did = self.db, self.cfg["id"]
                patch = {"live/desks/" + did: None}
                for r in ROOMS:
                    if dig(self.live, "assign", r) == did:
                        patch["live/assign/" + r] = None
                db.patch("", patch)
            except Exception:
                pass
        self.cfg["geometry"] = self.root.geometry()
        save_json(CONFIG_FILE, self.cfg)
        for s in self.streams:
            s.stop()
        self.runner.close()
        self.root.destroy()


if __name__ == "__main__":
    DeskApp().root.mainloop()
