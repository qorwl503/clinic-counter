# -*- coding: utf-8 -*-
"""진료 카운터 실행기 — 켤 때 새 버전이 있으면 받아서 바꾸고 프로그램을 실행합니다.
임시 폴더를 쓰지 않는 폴더형 실행 파일이라 'Failed to remove temporary directory' 오류가 나지 않습니다.
같은 실행기를 '데스크.exe' / '진료실.exe' 이름으로 두면 이름에 따라 해당 프로그램을 실행합니다."""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

BASE_URL = os.environ.get("CLINIC_UPDATE_URL", "https://raw.githubusercontent.com/qorwl503/clinic-counter/build/")
FONT = "Malgun Gothic"

HERE = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
NAME = Path(sys.executable if getattr(sys, "frozen", False) else sys.argv[0]).stem
MODE = os.environ.get("CLINIC_MODE") or ("room" if ("진료실" in NAME or "room" in NAME.lower()) else "desk")
APP = HERE / "app"


def current():
    try:
        return (APP / "current.txt").read_text(encoding="ascii").strip()
    except OSError:
        return ""


def target_exe(ver):
    return APP / ver / MODE / ("%s.exe" % MODE)


def fetch(url, timeout):
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def check_update(show=True):
    """새 버전이 있으면 app/<버전> 폴더에 받아 두고 current.txt를 바꿈. 바꿨으면 True."""
    try:
        latest = fetch(BASE_URL + "version.txt?t=%d" % time.time(), 5).decode("ascii").strip()
    except Exception:
        return False
    cur = current()
    if len(latest) < 7 or latest == cur:
        return False
    guard = APP / "update_try.json"
    try:
        g = json.loads(guard.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        g = {}
    if g.get("target") == latest and time.time() - g.get("at", 0) < 600:
        return False                      # 방금 실패한 업데이트는 10분 동안 다시 시도하지 않음
    APP.mkdir(parents=True, exist_ok=True)
    guard.write_text(json.dumps({"target": latest, "at": time.time()}), encoding="utf-8")

    win = None
    if show:
        try:
            import tkinter as tk
            win = tk.Tk()
            win.title("진료 카운터 업데이트")
            win.attributes("-topmost", True)
            win.geometry("340x100")
            tk.Label(win, text="새 버전으로 업데이트 중입니다…\n잠시만 기다려 주세요 (10~30초)",
                     font=(FONT, 11)).pack(expand=True)
            win.update()
        except Exception:
            win = None
    tmp_zip = APP / ("_download_%s.zip" % latest)
    tmp_dir = APP / ("_new_%s" % latest)
    ok = False
    try:
        data = fetch(BASE_URL + "app-%s.zip" % latest, 180)
        tmp_zip.write_bytes(data)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        with zipfile.ZipFile(tmp_zip) as z:
            z.extractall(tmp_dir)
        if not ((tmp_dir / "desk" / "desk.exe").exists() and (tmp_dir / "room" / "room.exe").exists()):
            raise ValueError("업데이트 파일 내용이 올바르지 않습니다")
        final = APP / latest
        shutil.rmtree(final, ignore_errors=True)
        os.replace(str(tmp_dir), str(final))
        (APP / "current.txt").write_text(latest, encoding="ascii")
        ok = True
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    finally:
        try:
            tmp_zip.unlink()
        except OSError:
            pass
        if win is not None:
            try:
                win.destroy()
            except Exception:
                pass
    if ok:
        cleanup(keep={latest, cur})
    return ok


def cleanup(keep):
    """지난 버전 폴더 정리 (사용 중이면 다음에 다시 시도)"""
    for d in APP.iterdir():
        if d.is_dir() and d.name not in keep and not d.name.startswith("_"):
            shutil.rmtree(d, ignore_errors=True)


def launch():
    ver = current()
    exe = target_exe(ver) if ver else None
    if not exe or not exe.exists():
        try:
            import tkinter as tk
            from tkinter import messagebox
            r = tk.Tk()
            r.withdraw()
            messagebox.showerror("진료 카운터", "프로그램 파일을 찾지 못했습니다.\n인터넷 연결 후 다시 실행하거나, 설치 파일을 다시 받아 주세요.")
        except Exception:
            pass
        return False
    if os.environ.get("CLINIC_LAUNCHER_DRYRUN"):
        print("LAUNCH", exe)
        return True
    subprocess.Popen([str(exe)], cwd=str(exe.parent), close_fds=True,
                     creationflags=0x00000008 | 0x00000200)   # DETACHED_PROCESS | NEW_PROCESS_GROUP
    return True


if __name__ == "__main__":
    check_update(show=not os.environ.get("CLINIC_LAUNCHER_DRYRUN"))
    launch()
