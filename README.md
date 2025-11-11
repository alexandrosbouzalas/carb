# carb — Content-addressable robust backup + per-file ingester with PAR2

`carb` is a single-file, portable Bash tool that ingests files from a start directory into a **content-addressed** store, **deduplicates** by `(size, sha256)`, and optionally generates **PAR2** parity sets for robust integrity verification and repair.  
Each run creates a **self-contained metadata bundle** and a **recovery script** that can reconstruct the original tree and verify/repair bytes during restore.

> **Key points**
> - **Fast** — Parallel ingestion and SHA-256 streaming  
> - **Safe** — Non-destructive, append-only blob storage  
> - **Deduplicated** — Identical files hardlink to the same blob  
> - **Robust** — Optional PAR2 parity for bitrot repair  
> - **Portable** — POSIX shell, works on Linux and macOS  
> - **Self-documenting** — Generates logs and recovery scripts

---

## 🧩 Why carb?

Backups often fail quietly: corruption, silent bitrot, or missing files.  
`carb` treats **bytes** as the ultimate source of truth:

- Uses **content-addressable** storage to prevent duplication.
- Creates **verifiable** blobs with optional PAR2 redundancy.
- Keeps **append-only** logs so every run is auditable.
- Avoids complex backup formats — data remains as plain files.

---

## ⚙️ How it works

1. **Scan**: Walks the target directory, pruning internal carb folders and excluded patterns.  
2. **Ingest**: For each regular file:
   - Streams bytes into a temporary copy and computes SHA-256.
   - Names the blob as `<18-digit-padded-size>_<sha256>.data`.
   - Deduplicates using hardlinks (no duplicates ever stored twice).
   - Optionally detects MIME type using `file`.
   - Optionally creates **PAR2** parity for self-healing.
3. **Record**: Logs per-run metadata under `blobs_meta/v05_<timestamp>/`.
4. **Recover**: Generates a `recover.sh` script capable of restoring all files and verifying with PAR2.

---

## 📦 Installation

### Requirements
**Required**
- `bash`, `find`, `xargs`, `awk`, `sed`, `tee`, `mktemp`, `ln`, `cp`, `stat`, `date`
- `openssl` **or** `shasum`

**Optional**
- `file` (for MIME detection)
- `par2cmdline` (`par2create` or `par2`) for parity verification and repair


> **INFO** : If the demo script complains that par2 is missing and automatic installation fails, try to manually install it.

Example:

```bash
sudo apt install par2
```

### Auto-install
If missing dependencies are detected, `carb` can attempt installation via:
`apt`, `dnf`, `yum`, `pacman`, `zypper`, `apk`, `brew`, or `port`.

To clone:
```bash
git clone https://github.com/alexandrosbouzalas/carb.git
cd carb
chmod +x carb.sh
./install.sh
```

To verify functionality:
```bash
chmod +x demo.sh
./demo.sh
```