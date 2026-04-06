"""
TG Dashboard Patch Script
Jalankan: python patch.py
Otomatis patch semua file dan restart server.
"""
import os, sys, ast, subprocess, shutil

BASE = os.path.dirname(os.path.abspath(__file__))
BACKEND = r"C:\Users\user\Downloads\telegrammm\web-telemarketing\backend"

def patch_file(path, old, new, label):
    with open(path, encoding='utf-8') as f:
        content = f.read()
    if old not in content:
        print(f"  ⚠️  {label}: pola tidak ditemukan (mungkin sudah dipatch)")
        return False
    content = content.replace(old, new, 1)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"  ✅ {label}")
    return True

def verify_syntax(path):
    with open(path, encoding='utf-8') as f:
        src = f.read()
    try:
        ast.parse(src)
        return True
    except SyntaxError as e:
        print(f"  ❌ Syntax error di {path}: {e}")
        return False

print("\n=== TG Dashboard Patch ===\n")

patches = []

# ─── Tambahkan patch baru di bawah ini ───────────────────────────────
# Format: patches.append((file_relatif, old_str, new_str, label))
# ─────────────────────────────────────────────────────────────────────

ok = True
files_changed = set()
for rel_path, old, new, label in patches:
    full_path = os.path.join(BACKEND, rel_path.replace('/', os.sep))
    if not os.path.exists(full_path):
        print(f"  ❌ File tidak ditemukan: {full_path}")
        ok = False
        continue
    if patch_file(full_path, old, new, label):
        files_changed.add(full_path)

for path in files_changed:
    if not verify_syntax(path):
        ok = False

if not ok:
    print("\n❌ Ada error — server tidak direstart.")
    sys.exit(1)

if not patches:
    print("Tidak ada patch yang didaftarkan.")
    sys.exit(0)

print(f"\n✅ Semua patch OK ({len(files_changed)} file diubah)")
print("→ Restart server...")
os.chdir(BACKEND)
subprocess.Popen(["taskkill", "/F", "/IM", "python.exe"], 
                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
import time; time.sleep(2)
subprocess.Popen([sys.executable, "app.py"])
print("✅ Server dijalankan ulang.")
