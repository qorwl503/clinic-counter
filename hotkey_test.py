# GitHub Actions(Windows)에서 전역 단축키(F12 등)가 실제로 잡히는지 시험
import importlib.machinery, importlib.util, queue, time, ctypes, sys
loader = importlib.machinery.SourceFileLoader("room", "진료실.pyw")
spec = importlib.util.spec_from_loader("room", loader)
m = importlib.util.module_from_spec(spec)
loader.exec_module(m)
u = ctypes.windll.user32

def press(vk, mods=()):
    for mk in mods:
        u.keybd_event(mk, 0, 0, 0)
    u.keybd_event(vk, 0, 0, 0)
    time.sleep(0.05)
    u.keybd_event(vk, 0, 2, 0)
    for mk in reversed(mods):
        u.keybd_event(mk, 0, 2, 0)

q = queue.Queue()
t = m.HotkeyThread([("call", 0, 0x7B, "F12"), ("task0", 0, 0x75, "F6"), ("task1", 2, 0x76, "Ctrl+F7")], q)
t.start()
t.ready.wait(5)
time.sleep(0.5)
res = []
for vk, mods, want in ((0x7B, (), "call"), (0x75, (), "task0"), (0x76, (0x11,), "task1"), (0x76, (), None)):
    press(vk, mods)
    got = []
    end = time.time() + 2
    while time.time() < end:
        try:
            k, v = q.get(timeout=0.2)
            if k == "key":
                got.append(v)
        except queue.Empty:
            pass
    ok = (got == [want]) if want else (got == [])
    res.append(ok)
    print("vk=%s mods=%s expect=%s got=%s -> %s" % (hex(vk), mods, want, got, "OK" if ok else "FAIL"))
t.stop()
print("RESULT:", "ALL OK" if all(res) else "SOME FAILED")
