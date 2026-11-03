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
BUILD_VERSION = "dev"   # GitHub에서 exe를 만들 때 자동으로 바뀜
UPDATE_BASE = "https://raw.githubusercontent.com/qorwl503/clinic-counter/build/"
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
        return normalize(json.loads(raw.decode("utf-8"))) if raw else None

    def get(self, path, params=None):
        return self._req("GET", path, params=params, timeout=15)

    def get_etag(self, path):
        """(값, ETag) — 동시에 두 곳에서 고치는 것을 막는 조건부 쓰기용"""
        req = urllib.request.Request(self.full(path), headers={"X-Firebase-ETag": "true"})
        try:
            with urllib.request.urlopen(req, timeout=6, context=self.ctx) as r:
                raw, tag = r.read(), r.headers.get("ETag")
        except (urllib.error.URLError, OSError) as e:
            raise ConnectionError("인터넷 연결 실패: %s" % getattr(e, "reason", e))
        return (normalize(json.loads(raw.decode("utf-8"))) if raw else None), tag

    def put_if(self, path, value, etag):
        """ETag가 그대로일 때만 저장. 다른 곳에서 먼저 바꿨으면 False"""
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.full(path), data=data, method="PUT",
                                     headers={"Content-Type": "application/json", "if-match": etag})
        try:
            with urllib.request.urlopen(req, timeout=6, context=self.ctx) as r:
                r.read()
            return True
        except urllib.error.HTTPError as e:
            if e.code == 412:
                return False
            raise ConnectionError("서버 오류 %s" % e.code)
        except (urllib.error.URLError, OSError) as e:
            raise ConnectionError("인터넷 연결 실패: %s" % getattr(e, "reason", e))

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
        # 소켓을 다른 스레드에서 강제로 닫으면 Windows에서 멈출 수 있어 플래그만 세움
        self.stopped.set()

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
                            self.out.put((self.tag, event, msg.get("path", "/"), normalize(msg.get("data"))))
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


def normalize(v):
    """Firebase는 키가 1,2처럼 숫자면 배열로 돌려줌 -> 항상 dict로 변환"""
    if isinstance(v, list):
        return {str(i): normalize(x) for i, x in enumerate(v) if x is not None}
    if isinstance(v, dict):
        return {k: normalize(x) for k, x in v.items()}
    return v


def mirror_apply(root, event, path, data):
    """로컬 사본(dict)에 put/patch 반영. 새 root 반환."""
    keys = [k for k in path.strip("/").split("/") if k]
    if event == "put":
        if not keys:
            data = normalize(data)
            return data if isinstance(data, dict) else {}
        node = root if isinstance(root, dict) else {}
        root = node
        for k in keys[:-1]:
            if isinstance(node.get(k), list):
                node[k] = normalize(node[k])
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


ONLINE_SEC = 15          # 데스크 신호가 이 시간 넘게 없으면 꺼진 것으로 봄
STALE_CALL_SEC = 180     # 이보다 오래된 호출은 울리지 않음


def desk_alive(live, desk_id, cache):
    """데스크가 켜져 있는지: 서버 시각 기록이 최근이거나, 값이 최근에 바뀌었으면 켜짐"""
    info = dig(live, "desks", desk_id)
    if not isinstance(info, dict):
        return False
    seen, now = info.get("seen"), time.time()
    prev = cache.get(desk_id)
    if prev is None or prev[0] != seen:
        cache[desk_id] = prev = (seen, now)
    fresh_local = now - prev[1] < ONLINE_SEC
    fresh_clock = isinstance(seen, (int, float)) and abs(now * 1000 - seen) < ONLINE_SEC * 1000
    return fresh_local or fresh_clock


def call_age(call):
    """호출 후 지난 초 (오늘 호출이 아니면 큰 값)"""
    try:
        if call.get("d") != today():
            return 10 ** 9
        t = datetime.combine(date.today(), datetime.strptime(call.get("time", ""), "%H:%M:%S").time())
        return (datetime.now() - t).total_seconds()
    except (ValueError, AttributeError):
        return 10 ** 9


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
    "무음": [],
    "딩동": [(988, 0.25), (784, 0.45)],
    "딩동 (높게)": [(1319, 0.25), (1047, 0.45)],
    "딩동 (낮게)": [(659, 0.28), (523, 0.5)],
    "딩동 (아주 낮게)": [(494, 0.3), (392, 0.55)],
    "딩동 (빠르게)": [(988, 0.14), (784, 0.28)],
    "딩동 (느리게)": [(988, 0.45), (0, 0.08), (784, 0.8)],
    "딩동 (낮게·느리게)": [(659, 0.45), (0, 0.08), (523, 0.85)],
    "딩동 (낮게·빠르게)": [(659, 0.15), (523, 0.3)],
    "딩동 두 번": [(988, 0.2), (784, 0.35), (0, 0.15), (988, 0.2), (784, 0.35)],
    "딩동 두 번 (낮게)": [(659, 0.22), (523, 0.38), (0, 0.15), (659, 0.22), (523, 0.38)],
    "딩동댕": [(784, 0.22), (988, 0.22), (1175, 0.45)],
    "번호표 띵동": [(1175, 0.35, "bell"), (932, 0.7, "bell")],
    "번호표 띵동 (낮게)": [(784, 0.38, "bell"), (622, 0.75, "bell")],
    # --- 2음 추가 ---
    "동딩 (올라가는)": [(784, 0.25), (988, 0.45)],
    "동딩 (올라가는·낮게)": [(523, 0.28), (659, 0.5)],
    "딩동 (넓은 간격)": [(1047, 0.25), (698, 0.5)],
    "딩동 (옥타브)": [(1047, 0.25), (523, 0.55)],
    "띵동 종소리 (높게)": [(1568, 0.3, "bell"), (1245, 0.65, "bell")],
    "띵동 종소리 (아주 낮게)": [(587, 0.4, "bell"), (466, 0.8, "bell")],
    "통통 마림바": [(784, 0.22, "marimba"), (587, 0.4, "marimba")],
    "통통 마림바 (낮게)": [(523, 0.24, "marimba"), (392, 0.45, "marimba")],
    "또롱 (올라가는 마림바)": [(659, 0.18, "marimba"), (988, 0.4, "marimba")],
    "띠링 (짧고 높게)": [(1568, 0.12, "marimba"), (2093, 0.3, "marimba")],
    "딩-동 (길게 울림)": [(880, 0.6, "bell"), (0, 0.05), (659, 1.1, "bell")],
    "딩동 (톡톡 짧게)": [(1175, 0.1), (0, 0.04), (880, 0.16)],
}
DEFAULT_SOUNDS = {"1": "딩동", "2": "딩동 (낮게)", "staff1": "딩동 두 번", "staff2": "딩동 두 번 (낮게)",
                  "staff": "딩동 두 번", "msg": "무음"}
DEFAULT_MSG_BUTTONS = ["대기", "폐암사후", "종검결과"]


def msg_text(label):
    """데스크 → 진료실 알림 문구"""
    label = (label or "").strip()
    if label in ("", "대기", "환자 대기"):
        return "과장님, 환자 대기중입니다"
    if not label.endswith("상담"):
        label += " 상담"
    return "과장님, %s 대기중입니다" % label


def sound_file(name):
    """음 목록을 wav 파일로 만들어 경로 반환 (한 번만 생성)"""
    import wave, math, struct
    notes = SOUNDS.get(name)
    if not isinstance(notes, list) or not notes:
        return None
    import hashlib
    path = DATA_DIR / "sounds" / ("%s.wav" % hashlib.md5(name.encode("utf-8")).hexdigest()[:10])
    if path.exists():
        return path
    rate, frames = 22050, bytearray()
    for note in notes:
        freq, dur = note[0], note[1]
        kind = note[2] if len(note) > 2 else "sine"
        n = int(rate * dur)
        for k in range(n):
            if freq:
                ph = 2 * math.pi * freq * k / rate
                if kind == "bell":      # 종소리: 배음 많고 길게 울림
                    env = min(1.0, k / (rate * 0.005)) * math.exp(-2.2 * k / n)
                    v = env * (0.5 * math.sin(ph) + 0.25 * math.sin(2.76 * ph) +
                               0.12 * math.sin(5.4 * ph) + 0.08 * math.sin(2 * ph))
                elif kind == "square":  # 부저·진동벨
                    env = min(1.0, k / (rate * 0.005)) * (1.0 if k < n * 0.85 else (n - k) / (n * 0.15))
                    v = env * 0.35 * (1 if math.sin(ph) >= 0 else -1)
                elif kind == "marimba":  # 짧고 둥근 소리
                    env = min(1.0, k / (rate * 0.003)) * math.exp(-7.0 * k / n)
                    v = env * (0.7 * math.sin(ph) + 0.15 * math.sin(4 * ph))
                else:
                    env = min(1.0, k / (rate * 0.01)) * math.exp(-3.0 * k / n)
                    v = env * (0.6 * math.sin(ph) + 0.2 * math.sin(2 * ph))
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
    if name == "무음":
        return
    if name not in SOUNDS:      # 예전에 고른 소리가 없어졌으면 기본 딩동
        name = "딩동"
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


def quit_now(root, cfg_path=None, cfg=None, last_job=None):
    """창을 바로 닫고 프로세스 종료 (네트워크 대기 때문에 '응답 없음'이 뜨지 않도록)"""
    try:
        if cfg_path is not None and cfg is not None:
            cfg["geometry"] = root.geometry()
            save_json(cfg_path, cfg)
        root.withdraw()
        root.update()
    except Exception:
        pass
    if last_job:
        t = threading.Thread(target=last_job, daemon=True)
        t.start()
        t.join(2.0)
    os._exit(0)


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
    var = tk.StringVar(master=win, value=current)
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
        self.win.bind("<Configure>", lambda e: (self.summary.config(wraplength=max(300, self.win.winfo_width() - 30)),
                                                self.detail.config(wraplength=max(300, self.win.winfo_width() - 30))))
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
            lines.append("진료실 %s — 진료 완료 %d명, 업무 %d건" % (r, p, sum(t)))
            lines.append("    " + summary_text(t))
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
def auto_update(exe_name):
    """켤 때 새 버전이 있으면 내려받아 바꾸고 다시 실행. 업데이트했으면 이 프로세스는 종료됨."""
    if not getattr(sys, "frozen", False) or BUILD_VERSION == "dev":
        return
    exe = Path(sys.executable)
    old = exe.with_name(exe.name + ".old")
    try:
        if old.exists():
            old.unlink()
    except OSError:
        pass
    try:
        req = urllib.request.Request(UPDATE_BASE + "version.txt?t=%d" % time.time(),
                                     headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=5) as r:
            latest = r.read().decode("utf-8").strip()
    except Exception:
        return
    if not latest or latest == BUILD_VERSION or len(latest) < 7:
        return
    win = tk.Tk()
    win.title("업데이트")
    win.attributes("-topmost", True)
    win.geometry("320x90")
    tk.Label(win, text="새 버전으로 업데이트 중입니다…\n잠시만 기다려 주세요.", font=(FONT, 11)).pack(expand=True)
    win.update()
    new = exe.with_name(exe.name + ".new")
    try:
        with urllib.request.urlopen(UPDATE_BASE + exe_name, timeout=90) as r:
            data = r.read()
        if len(data) < 1000000 or data[:2] != b"MZ":
            raise ValueError("잘못된 파일")
        new.write_bytes(data)
        os.replace(str(exe), str(old))       # 실행 중인 exe도 이름 변경은 가능
        os.replace(str(new), str(exe))
    except Exception:
        try:
            if not exe.exists() and old.exists():
                os.replace(str(old), str(exe))
            if new.exists():
                new.unlink()
        except OSError:
            pass
        win.destroy()
        return
    win.destroy()
    import subprocess
    env = dict(os.environ, PYINSTALLER_RESET_ENVIRONMENT="1")
    subprocess.Popen([str(exe)], env=env, close_fds=True,
                     creationflags=0x00000008 | 0x00000200)   # DETACHED_PROCESS | NEW_PROCESS_GROUP
    os._exit(0)


# ===== 공통 모듈 끝 =====

# ===== 데스크 프로그램 =====
CONFIG_FILE = DATA_DIR / "desk.json"


class DeskApp:
    def __init__(self):
        self.cfg = load_json(CONFIG_FILE, {})
        if not self.cfg.get("id"):
            self.cfg["id"] = "d" + secrets.token_hex(6)
            self.cfg.setdefault("name", "데스크")
            save_json(CONFIG_FILE, self.cfg)
        self.root = tk.Tk()
        self.root.title("데스크 · 대기/호출 " + VERSION + " (" + BUILD_VERSION[:7] + ")")
        self.root.geometry(self.cfg.get("geometry") or "660x720+60+40")
        self.root.minsize(600, 560)
        self.root.configure(bg=BG)
        self.top_var = tk.BooleanVar(master=self.root, value=self.cfg.get("topmost", True))
        self.root.attributes("-topmost", self.top_var.get())
        self.runner = Runner(self.root, workers=3)
        self.inbox = queue.Queue()
        self.db = None
        self.streams = []
        self.live = {}
        self.events = {}
        self.legacy = {r: {} for r in ROOMS}
        self.events_day = None
        self.online = False
        self.last_call = {r: None for r in ROOMS}
        self.popups = {}
        self.unacked = set()
        self.staff_popups = {}
        self.staff_unacked = set()
        self.last_staff = {r: None for r in ROOMS}
        self.notes = {r: "" for r in ROOMS}
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
        self.claimed_on_start = False
        self.last_call = {r: None for r in ROOMS}
        self.last_staff = {r: None for r in ROOMS}
        self.notes = {r: "" for r in ROOMS}
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
        self.legacy = {r: {} for r in ROOMS}
        self.streams = [Stream(self.db, "live", self.inbox, "live"),
                        Stream(self.db, "days/" + self.events_day, self.inbox, "events")]
        self.streams += [Stream(self.db, "legacy/%s/%s" % (r, self.events_day), self.inbox, "legacy" + r)
                         for r in ROOMS]
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
                if tag.startswith("legacy"):
                    r = tag[6:]
                    self.legacy[r] = legacy_apply(self.legacy.get(r), ev, path, data)
                elif tag == "live":
                    self.live = mirror_apply(self.live, ev, path, data)
                    if not self.claimed_on_start and ev == "put" and path == "/":
                        self.claimed_on_start = True
                        self.root.after(500, self.claim_free_rooms)
                else:
                    self.events = mirror_apply(self.events, ev, path, data)
                changed = True
        except queue.Empty:
            pass
        if changed:
            self.check_calls()
            self.check_staff()
            self.render()
        self.root.after(100, self.pump)

    def set_status(self, text, color):
        if color == "#a44b4b":   # 끊김: 크게, 빨간 바탕
            self.w["status"].config(text="⚠ " + text + "  — 화면 숫자가 최신이 아닐 수 있습니다",
                                    fg="white", bg="#c0392b", font=(FONT, 11, "bold"))
        else:
            self.w["status"].config(text=text, fg=color, bg=BG, font=(FONT, 9))
        if hasattr(self, "notes"):
            self.render()

    # ---------- 화면 ----------
    def build(self):
        f = tk.Frame(self.root, bg=BG, padx=14, pady=10)
        f.pack(fill="both", expand=True)
        top = tk.Frame(f, bg=BG)
        top.pack(fill="x")
        self.w["title"] = tk.Label(top, text="데스크", bg=BG, font=(FONT, 15, "bold"), fg="#18263b")
        self.w["title"].pack(side="left")
        tk.Button(top, text="⚙", relief="flat", bg="#e8eef9", command=self.settings).pack(side="right")
        tk.Button(top, text="📊 통계", relief="flat", bg="#e8eef9", command=self.stats).pack(side="right", padx=6)
        self.w["status"] = tk.Label(f, bg=BG, anchor="w", font=(FONT, 9), padx=4)
        self.w["status"].pack(fill="x", pady=(2, 6))

        mine = tk.Frame(f, bg="#fff8e6", padx=10, pady=7)
        mine.pack(fill="x", pady=(0, 6))
        tk.Label(mine, text="내 담당:", bg="#fff8e6", font=(FONT, 10, "bold")).pack(side="left")
        saved = self.cfg.get("my_rooms")
        if saved is None:   # 이전 버전 설정 이어받기
            v = self.cfg.get("view", "")
            saved = list(ROOMS) if v == "both" else ([v] if v in ROOMS else [])
        self.sel = {}
        for r in ROOMS:
            var = tk.BooleanVar(master=self.root, value=r in saved)
            self.sel[r] = var
            tk.Checkbutton(mine, text="진료실 " + r, variable=var, bg="#fff8e6", font=(FONT, 10, "bold"),
                           command=self.set_mode).pack(side="left", padx=4)
        tk.Label(mine, text="(둘 다 체크 = 전체)", bg="#fff8e6", fg="#7a6a3a", font=(FONT, 8)).pack(side="left")

        cols = tk.Frame(f, bg=BG)
        cols.pack(fill="both", expand=True)
        self.cols = cols
        self.layout = None
        for ci, r in enumerate(ROOMS):
            cols.grid_columnconfigure(ci, weight=1, uniform="room")
            box = tk.Frame(cols, bg="#eaf2fc", padx=10, pady=8)
            box.grid(row=0, column=ci, sticky="nsew", padx=(0, 6) if ci == 0 else (6, 0))
            head = tk.Frame(box, bg="#eaf2fc")
            head.pack(fill="x")
            tk.Label(head, text="진료실 " + r, bg="#eaf2fc", font=(FONT, 14, "bold")).pack(side="left")
            own = tk.Label(head, bg="#eaf2fc", font=(FONT, 8))
            own.pack(side="right")
            big = tk.Label(box, bg="#eaf2fc", fg="#1d62b2", font=(FONT, 22, "bold"), anchor="w")
            big.pack(fill="x", pady=(2, 0))
            waitinfo = tk.Label(box, bg="#eaf2fc", fg="#56647b", font=(FONT, 10, "bold"), anchor="w",
                                justify="left")
            waitinfo.pack(fill="x")
            done = tk.Label(box, bg="#eaf2fc", fg="#24824d", font=(FONT, 12, "bold"), anchor="w")
            done.pack(fill="x")
            call = tk.Label(box, bg="#eaf2fc", fg="#56647b", font=(FONT, 9), anchor="w",
                            justify="left", wraplength=280)
            call.pack(fill="x", pady=(2, 5))
            btns = tk.Frame(box, bg="#eaf2fc")
            btns.pack(fill="x")
            tk.Button(btns, text="＋ 대기 등록", bg="#cfe3ff", font=(FONT, 12, "bold"), relief="raised", bd=3,
                      activebackground="#b5d3ff", cursor="hand2",
                      command=lambda r=r: self.add(r)).pack(side="left", fill="x", expand=True)
            tk.Button(btns, text="− 취소", bg="#f1f2f5", font=(FONT, 10), relief="raised", bd=3, cursor="hand2",
                      command=lambda r=r: self.remove(r)).pack(side="left", padx=(5, 0))
            ack = tk.Button(box, text="호출 확인", bg="#ffcf9e", font=(FONT, 11, "bold"), relief="raised", bd=3,
                            cursor="hand2",
                            command=lambda r=r: self.ack(r))
            mb = tk.Frame(box, bg="#eaf2fc")
            mb.pack(fill="x", pady=(6, 0))
            tk.Label(mb, text="진료실 알림", bg="#eaf2fc", fg="#65738a", font=(FONT, 8)).pack(anchor="w")
            mbr = tk.Frame(mb, bg="#eaf2fc")
            mbr.pack(fill="x")
            for label in (self.cfg.get("msg_buttons") or DEFAULT_MSG_BUTTONS):
                tk.Button(mbr, text=label, bg="#ffe9b0", activebackground="#ffd97a", relief="raised", bd=3,
                          cursor="hand2", font=(FONT, 10, "bold"), padx=6,
                          command=lambda r=r, label=label: self.send_msg(r, label)).pack(
                    side="left", padx=(0, 4), pady=1)
            msginfo = tk.Label(mb, bg="#eaf2fc", fg="#8a6d1d", font=(FONT, 8), anchor="w")
            msginfo.pack(fill="x")
            tl = tk.Frame(box, bg="#eaf2fc")
            tl.pack(fill="x", pady=(8, 0))
            tk.Label(tl, text="업무별 (누르면 상세)", bg="#eaf2fc", fg="#65738a",
                     font=(FONT, 8)).pack(anchor="w")
            rows = []
            for i, name in enumerate(NAMES):
                lab = tk.Label(tl, bg="white", fg="#22324a", font=(FONT, 9), anchor="w",
                               padx=6, cursor="hand2")
                lab.pack(fill="x", pady=1)
                lab.bind("<Button-1>", lambda e, r=r, i=i: self.task_detail(r, i))
                rows.append(lab)
            self.w[r] = {"box": box, "own": own, "big": big, "done": done, "call": call,
                         "ack": ack, "head": head, "btns": btns, "tl": tl, "rows": rows,
                         "mb": mb, "mbr": mbr, "msginfo": msginfo, "waitinfo": waitinfo}

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
        return desk_alive(self.live, desk_id, self.seen)

    def my_rooms(self):
        return [r for r in ROOMS if self.sel[r].get()]

    def owners(self, room):
        """이 진료실을 고른 켜진 데스크들 [(id, 이름)] — 나 포함"""
        out = []
        for did, info in (dig(self.live, "desks") or {}).items():
            if did == self.cfg["id"]:
                continue
            if isinstance(info, dict) and (info.get("rooms") or {}).get(room) and self.desk_online(did):
                out.append((did, info.get("name") or "다른 데스크"))
        if room in self.my_rooms():
            out.insert(0, (self.cfg["id"], "나"))
        return out

    def should_alert(self, room):
        """(알림 여부, 담당 없음 여부): 내가 고른 진료실이면 알림, 아무도 안 골랐으면 모두 알림"""
        if room in self.my_rooms():
            return True, False
        if not self.owners(room):
            return True, True
        return False, False

    def render(self):
        self.w["title"].config(text=self.cfg.get("name", "데스크"))
        for r in ROOMS:
            w = self.w[r]
            node = dig(self.live, "rooms", r) or {}
            waiting = len(tickets_today(node))
            counts = [a + b for a, b in zip(task_counts(self.events, r),
                                            legacy_counts(getattr(self, "legacy", {}).get(r)))]
            patients = len(done_ids(self.events, r)) + sum(counts)
            if self.db is None or not self.live and not self.online:
                w["big"].config(text="대기 —")
            else:
                w["big"].config(text="대기 %d명" % waiting)
            w["done"].config(text="오늘 진료 완료 %d명" % patients)
            # 대기 시간: 먼저 온 순서대로 몇 분째인지
            q = (node.get("queue") or {}) if isinstance(node.get("queue"), dict) else {}
            mins = [self.minutes_since(q[t].get("t", "")) for t in tickets_today(node)]
            mins = [m for m in mins if m is not None]
            if mins:
                top = mins[0]
                more = " · ".join("%d분" % m for m in mins[1:4]) + (" …" if len(mins) > 4 else "")
                w["waitinfo"].config(
                    text="⏱ 첫 대기 %d분째%s" % (top, ("   (다음: %s)" % more) if more else ""),
                    fg="#c0392b" if top >= 30 else ("#c77923" if top >= 15 else "#56647b"))
            else:
                w["waitinfo"].config(text="", fg="#56647b")
            stale = not self.online
            w["big"].config(fg="#9aa6b8" if stale else "#1d62b2")
            w["done"].config(fg="#9aa6b8" if stale else "#24824d")
            for i, lab in enumerate(w["rows"]):
                lab.config(text="%s   %d" % (NAMES[i], counts[i]),
                           fg="#22324a" if counts[i] else "#9aa6b8")
            call = node.get("call") if isinstance(node.get("call"), dict) else None
            if call and call.get("d") == today():
                mins = self.minutes_since(call.get("time", ""))
                txt = "마지막 호출 %s" % call.get("time", "")[:5] + (" (%d분 전)" % mins if mins is not None else "")
            else:
                txt = "오늘 호출 없음"
            ows = self.owners(r)
            if ows:
                w["own"].config(text="담당: " + ", ".join(n for _, n in ows),
                                fg="#21804c" if ows[0][1] == "나" else "#1d62b2")
            else:
                w["own"].config(text="담당 없음 → 모든 데스크에 알림", fg="#bd4429")
            alert = r in self.unacked
            color = "#ffe2c4" if alert else "#eaf2fc"
            m = node.get("msg") if isinstance(node.get("msg"), dict) else None
            if m and m.get("d") == today() and call_age(m) < 1800:
                w["msginfo"].config(text="%s '%s' 보냄 · %s" % (
                    m.get("time", "")[:5], m.get("label", ""),
                    "✔ 과장님 확인" if m.get("acked") else "확인 전"),
                    fg="#21804c" if m.get("acked") else "#8a6d1d")
            else:
                w["msginfo"].config(text="")
            for key in ("box", "head", "btns", "tl", "own", "big", "done", "call", "mb", "mbr", "msginfo", "waitinfo"):
                w[key].config(bg=color)
            for child in list(w["head"].winfo_children()) + list(w["tl"].winfo_children())[:1]:
                child.config(bg=color)
            note = self.notes.get(r, "") if hasattr(self, "notes") else ""
            if r in getattr(self, "staff_unacked", ()):
                note = "🙋 직원 호출 중!" + ("\n" + note if note else "")
            w["call"].config(text=("🔔 호출됨! 환자 안내 후 '호출 확인'\n" if alert else "") + txt +
                             ("\n" + note if note else ""),
                             fg="#bd4429" if (alert or note.startswith("⚠")) else "#56647b")
            if alert:
                w["ack"].pack(fill="x", pady=(5, 0), after=w["btns"])
            else:
                w["ack"].pack_forget()

        self.apply_layout()
        self.w["foot"].config(text="담당 없는 진료실은 모든 데스크에 알림")

    def apply_layout(self):
        """진료실 1만 담당 → 진료실 1만, 진료실 2만 → 진료실 2만, 전체/미선택 → 둘 다 표시"""
        show = tuple(self.my_rooms()) or ROOMS
        if show == self.layout:
            return
        self.layout = show
        for ci, r in enumerate(ROOMS):
            box = self.w[r]["box"]
            if r in show:
                pos = show.index(r)
                padx = (0, 0) if len(show) == 1 else ((0, 6) if pos == 0 else (6, 0))
                box.grid(row=0, column=pos, sticky="nsew", padx=padx)
            else:
                box.grid_remove()
        for ci in range(len(ROOMS)):
            self.cols.grid_columnconfigure(ci, weight=1 if ci < len(show) else 0,
                                           uniform="room" if ci < len(show) else "")
        # 창 너비 맞추기 (높이·위치는 그대로)
        try:
            self.root.update_idletasks()
            h = self.root.winfo_height()
            x, y = self.root.winfo_x(), self.root.winfo_y()
            width = 390 if len(show) == 1 else 660
            self.root.minsize(370 if len(show) == 1 else 600, 560)
            if h > 1:
                self.root.geometry("%dx%d+%d+%d" % (width, h, x, y))
        except tk.TclError:
            pass

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
        self.root.after(10000 if not self.unacked else 5000, self.tick)

    def heartbeat(self):
        if self.db:
            body = {"name": self.cfg.get("name", "데스크"), "seen": {".sv": "timestamp"},
                    "rooms": {r: True for r in self.my_rooms()} or None}
            db, did = self.db, self.cfg["id"]
            self.runner.submit(lambda: db.put("live/desks/" + did, body))
        self.root.after(5000, self.heartbeat)

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

    def claim_free_rooms(self):
        """(이전 방식 호환용 — 담당은 각 데스크가 독립적으로 고름)"""
        return

    def set_mode(self):
        """담당 선택: 각 데스크가 독립적으로 고르고, 여러 데스크가 같은 진료실을 골라도 모두 알림 받음."""
        self.cfg["my_rooms"] = self.my_rooms()
        save_json(CONFIG_FILE, self.cfg)
        self.render()
        if self.db:
            did = self.cfg["id"]
            rooms = {r: True for r in self.my_rooms()} or None
            self.live = mirror_apply(self.live, "put", "/desks/%s/rooms" % did, rooms)
            db = self.db
            self.runner.submit(lambda: db.put("live/desks/%s/rooms" % did, rooms), fail=self.fail)

    def select(self, rooms):
        """(시험용) 담당 진료실 지정"""
        for r in ROOMS:
            self.sel[r].set(r in rooms)
        self.set_mode()

    # ---------- 호출 알림 ----------
    def check_calls(self):
        for r in ROOMS:
            call = dig(self.live, "rooms", r, "call")
            if not isinstance(call, dict):
                call = {}
            cid = call.get("id")
            key = (cid, bool(call.get("cancelled")), bool(call.get("acked")))
            if self.last_call[r] is None:
                self.last_call[r] = key
                continue
            if not cid or key == self.last_call[r]:
                continue
            prev_id = self.last_call[r][0] if isinstance(self.last_call[r], tuple) else None
            self.last_call[r] = key
            if call.get("acked") and not call.get("cancelled"):   # 다른 데스크가 확인함
                if r in self.unacked:
                    self.ack(r, remote=True)
                    self.notes[r] = "%s에서 확인함" % call.get("acked")
                    self.render()
                continue
            if call.get("cancelled"):            # 의사가 호출 취소
                if cid == prev_id or r in self.unacked:
                    self.ack(r)
                self.notes[r] = "호출 취소됨 (%s 호출)" % call.get("time", "")[:5]
                self.render()
                continue
            if cid == prev_id or call.get("d") != today():
                continue
            if call_age(call) > STALE_CALL_SEC:   # 연결 끊긴 사이 지난 호출: 울리지 않음
                self.notes[r] = "⚠ 놓친 호출 %s (연결이 끊겨 있었음)" % call.get("time", "")[:5]
                self.render()
                continue
            self.notes[r] = ""
            again = bool(call.get("repeat"))
            go, unowned = self.should_alert(r)
            if go:
                self.alert(r, call.get("time", ""), unowned, again)

    # ---------- 데스크 → 진료실 알림 ----------
    def send_msg(self, r, label):
        if not self.need_db():
            return
        if not self.online:
            play_alert()
            self.set_status("연결이 끊겨 알림을 보낼 수 없습니다", "#a44b4b")
            return
        last = getattr(self, "last_msg_sent", {})
        if time.time() - last.get(r, 0) < 3:
            return
        last[r] = time.time()
        self.last_msg_sent = last
        val = {"id": new_id(), "label": label, "text": msg_text(label), "time": now_hms(),
               "d": today(), "from": self.cfg.get("name", "데스크")}
        self.live = mirror_apply(self.live, "put", "/rooms/%s/msg" % r, val)
        self.render()
        db = self.db
        self.runner.submit(lambda: db.put("live/rooms/%s/msg" % r, val), fail=self.fail)

    # ---------- 직원 호출 ----------
    def check_staff(self):
        for r in ROOMS:
            st = dig(self.live, "rooms", r, "staff")
            if not isinstance(st, dict):
                st = {}
            sid = st.get("id")
            key = (sid, bool(st.get("acked")))
            if self.last_staff[r] is None:
                self.last_staff[r] = key
                continue
            if not sid or key == self.last_staff[r]:
                continue
            prev_id = self.last_staff[r][0]
            self.last_staff[r] = key
            if st.get("acked"):
                if r in self.staff_unacked:
                    self.staff_ack(r, remote=True)
                    self.notes[r] = "직원 호출: %s 확인" % st.get("acked")
                    self.render()
                continue
            if sid == prev_id or st.get("d") != today():
                continue
            if call_age(st) > STALE_CALL_SEC:
                self.notes[r] = "⚠ 놓친 직원 호출 %s" % st.get("time", "")[:5]
                self.render()
                continue
            if self.should_alert(r)[0]:
                self.staff_alert(r, st.get("time", ""), bool(st.get("repeat")))

    def staff_alert(self, r, when, again):
        self.staff_unacked.add(r)
        snd = self.cfg.get("sound_staff" + r) or DEFAULT_SOUNDS["staff" + r]
        rep = int(self.cfg.get("repeat", 2))
        play_sound(snd)
        for k in range(1, rep):
            self.root.after(1600 * k, lambda snd=snd: play_sound(snd))
        old = self.staff_popups.pop(r, None)
        if old is not None:
            try:
                old.destroy()
            except tk.TclError:
                pass
        p = tk.Toplevel(self.root)
        self.staff_popups[r] = p
        p.title("진료실 %s 직원 호출" % r)
        sw = p.winfo_screenwidth()
        x = sw // 2 - 460 if r == "1" else sw // 2 + 20
        p.geometry("440x190+%d+330" % max(0, x))
        p.attributes("-topmost", True)
        bg, fg = "#ece4ff", "#5b2fa8"
        p.configure(bg=bg)
        tk.Label(p, text="🙋 진료실 %s" % r, font=(FONT, 22, "bold"), bg=bg, fg=fg).pack(pady=(18, 0))
        tk.Label(p, text="직원 호출%s — 진료실로 와주세요" % (" (다시)" if again else ""),
                 font=(FONT, 15, "bold"), bg=bg, fg=fg).pack()
        tk.Label(p, text="호출 %s" % when, font=(FONT, 10), bg=bg).pack(pady=(4, 6))
        tk.Button(p, text="확인", width=14, font=(FONT, 11, "bold"), bg="#d6c6ff",
                  command=lambda: self.staff_ack(r)).pack()
        p.protocol("WM_DELETE_WINDOW", lambda: self.staff_ack(r))
        self.render()

    def staff_ack(self, r, remote=False):
        was = r in self.staff_unacked
        self.staff_unacked.discard(r)
        p = self.staff_popups.pop(r, None)
        if p is not None:
            try:
                p.destroy()
            except tk.TclError:
                pass
        st = dig(self.live, "rooms", r, "staff")
        if was and not remote and self.db and isinstance(st, dict) and not st.get("acked"):
            name = self.cfg.get("name", "데스크")
            self.live = mirror_apply(self.live, "put", "/rooms/%s/staff/acked" % r, name)
            self.last_staff[r] = (st.get("id"), True)
            db = self.db
            self.runner.submit(lambda: db.put("live/rooms/%s/staff/acked" % r, name), fail=self.fail)
        self.render()

    def alert(self, r, when, unowned, again=False):
        self.unacked.add(r)
        snd = self.cfg.get("sound" + r) or DEFAULT_SOUNDS[r]
        rep = int(self.cfg.get("repeat", 2))
        play_sound(snd)
        for k in range(1, rep):
            self.root.after(1600 * k, lambda snd=snd: play_sound(snd))
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
        tk.Label(p, text="🔁 다시 호출 — 같은 환자 모셔주세요" if again else "다음 환자 모셔주세요",
                 font=(FONT, 15, "bold"), bg="#fff0df", fg="#a83725").pack()
        tk.Label(p, text="호출 %s%s" % (when, "  (담당 데스크 없음)" if unowned else ""),
                 font=(FONT, 10), bg="#fff0df").pack(pady=(4, 6))
        tk.Button(p, text="확인", width=14, font=(FONT, 11, "bold"), bg="#ffcf9e",
                  command=lambda: self.ack(r)).pack()
        p.protocol("WM_DELETE_WINDOW", lambda: self.ack(r))
        self.render()

    def ack(self, r, remote=False):
        was = r in self.unacked
        self.unacked.discard(r)
        call = dig(self.live, "rooms", r, "call")
        if was and not remote and self.db and isinstance(call, dict) and not call.get("acked"):
            # 환자를 들여보냄 → 대기 −1, 진료실·다른 데스크에 '확인' 알림
            name = self.cfg.get("name", "데스크")
            db = self.db
            self.live = mirror_apply(self.live, "put", "/rooms/%s/call/acked" % r, name)
            ids = tickets_today(dig(self.live, "rooms", r))
            if ids:   # 화면에 바로 반영
                self.live = mirror_apply(self.live, "put", "/rooms/%s/queue/%s" % (r, ids[0]), None)

            def job():
                for _ in range(3):
                    cur, tag = db.get_etag("live/rooms/%s/call" % r)
                    if not isinstance(cur, dict) or cur.get("acked") or cur.get("cancelled"):
                        return "skip"          # 다른 데스크가 먼저 확인함
                    queue_now = db.get("live/rooms/%s/queue" % r) or {}
                    tids = sorted(k for k, v in queue_now.items()
                                  if isinstance(v, dict) and v.get("d") == today())
                    new = dict(cur, acked=name)
                    if tids:
                        new["ticket"] = {"id": tids[0], "v": queue_now[tids[0]]}
                    if db.put_if("live/rooms/%s/call" % r, new, tag):
                        if tids:
                            db.delete("live/rooms/%s/queue/%s" % (r, tids[0]))
                        return "ok"
                return "skip"

            self.runner.submit(job, fail=self.fail)
        p = self.popups.pop(r, None)
        if p is not None:
            try:
                p.destroy()
            except tk.TclError:
                pass
        self.render()

    # ---------- 설정 / 통계 ----------
    def task_detail(self, r, i):
        if not self.need_db():
            return
        TaskDetail(self.root, self.db, self.runner, r, i, self.top_var.get())

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
        name = tk.StringVar(master=win, value=self.cfg.get("name", "데스크"))
        tk.Entry(win, textvariable=name, width=24, font=(FONT, 10)).pack(**pad)
        tk.Label(win, text="예: 데스크 1, 데스크 2 (호출 담당 표시에 사용)", fg="#65738a",
                 font=(FONT, 8)).pack(**pad)

        tk.Label(win, text="연결 키", font=(FONT, 10, "bold")).pack(pady=(14, 2), **pad)
        key = tk.StringVar(master=win, value=self.cfg.get("key", ""))
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

        tk.Label(win, text="알림음 (진료실마다·호출 종류마다 다르게)", font=(FONT, 10, "bold")).pack(pady=(14, 2), **pad)
        svars = {}
        for skey, label in (("sound1", "진료실 1 환자"), ("sound_staff1", "진료실 1 직원"),
                            ("sound2", "진료실 2 환자"), ("sound_staff2", "진료실 2 직원")):
            row2 = tk.Frame(win)
            row2.pack(pady=1, **pad)
            tk.Label(row2, text=label, width=12, anchor="w").pack(side="left")
            v = tk.StringVar(master=win, value=self.cfg.get(skey) or DEFAULT_SOUNDS[skey.replace("sound_", "").replace("sound", "")])
            svars[skey] = v
            ttk.Combobox(row2, textvariable=v, values=list(SOUNDS), state="readonly", width=18).pack(side="left")
            tk.Button(row2, text="▶ 미리 듣기", command=lambda v=v: play_sound(v.get())).pack(side="left", padx=6)
        tk.Label(win, text="진료실 알림 버튼 (쉼표로 구분)", font=(FONT, 10, "bold")).pack(pady=(14, 2), **pad)
        mbv = tk.StringVar(master=win, value=", ".join(self.cfg.get("msg_buttons") or DEFAULT_MSG_BUTTONS))
        tk.Entry(win, textvariable=mbv, width=50, font=(FONT, 10)).pack(**pad)
        tk.Label(win, text="예: 대기, 폐암사후, 종검결과 → 진료실에 '과장님, 폐암사후 상담 대기중입니다'로 표시 (저장 후 다시 켜면 반영)",
                 fg="#65738a", font=(FONT, 8)).pack(**pad)
        row3 = tk.Frame(win)
        row3.pack(pady=2, **pad)
        tk.Label(row3, text="반복 횟수", width=8, anchor="w").pack(side="left")
        rep = tk.StringVar(master=win, value=str(self.cfg.get("repeat", 2)))
        ttk.Combobox(row3, textvariable=rep, values=["1", "2", "3"], state="readonly", width=4).pack(side="left")

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
            for skey, v in svars.items():
                self.cfg[skey] = v.get()
            self.cfg["repeat"] = int(rep.get())
            labels = [x.strip() for x in mbv.get().replace("，", ",").split(",") if x.strip()]
            self.cfg["msg_buttons"] = labels[:6] or DEFAULT_MSG_BUTTONS
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
        job = None
        if self.db:
            db, did = self.db, self.cfg["id"]
            patch = {"live/desks/" + did: None}

            def job():
                try:
                    db._req("PATCH", "", patch, timeout=2)
                except Exception:
                    pass
        quit_now(self.root, CONFIG_FILE, self.cfg, job)


if __name__ == "__main__":
    auto_update("desk.exe")
    DeskApp().root.mainloop()
