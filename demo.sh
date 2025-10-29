#!/usr/bin/env bash
set -Eeuo pipefail

# --- 0) Clean playground ------------------------------------------------------
WORKDIR="$(mktemp -d)"; cd "$WORKDIR"
echo "Demo workspace: $WORKDIR"

# Locate global carb and stage a local copy so outputs are self-contained
CARB_BIN="$(command -v carb || true)"
if [[ -z "$CARB_BIN" ]]; then
  echo "ERROR: 'carb' not found in PATH. Install it first, then re-run this demo." >&2
  exit 1
fi
cp "$CARB_BIN" ./carb.sh
chmod +x ./carb.sh

# PAR2 availability (informational)
PAR2_BIN="$(command -v par2 || true)"
PAR2_CREATE_BIN="$(command -v par2create || true)"
echo "PAR2: par2=${PAR2_BIN:-<missing>} par2create=${PAR2_CREATE_BIN:-<missing>}"

# --- 1) Generate sample tree --------------------------------------------------
mkdir -p data/docs data/media data/tmp
echo "hello world" > data/docs/readme.txt
# ~1 KiB file
printf 'A%.0s' {1..1000} > data/docs/notes.txt
# 64 KiB random
dd if=/dev/urandom of=data/media/pic.bin bs=1K count=64 status=none 2>/dev/null
echo "ignore me" > data/tmp/scratch.swp

# --- 2) FULL run (exclude tmp-ish files). Capturing stderr for debugging ------
CARB_EXCLUDE_GLOBS="*.swp,.DS_Store" \
CARB_COMMENT="initial demo run" \
CARB_PAR2=1 \
./carb.sh data --full 2> full_run.stderr || true

# Basic artifacts
echo "== blobs:"; ls -1 blobs_sha256 | head || true

echo "== meta (latest):"
RUN_META="$(ls -1d blobs_meta/v05_* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META}" ]]; then
  echo "ERROR: No run metadata found; full run likely failed." >&2
  echo "---- carb FULL run stderr (tail) ----"
  tail -n 200 full_run.stderr || true
  exit 1
fi
echo "  $RUN_META"
echo "  carb_settings:"
sed -n '1p' "$RUN_META/carb_settings" || true

# Parity presence (if par2 available)
echo "== parity files:"
if compgen -G "blobs_par2/*.par2" >/dev/null; then
  ls -1 blobs_par2 | head || true
else
  echo "(none yet) — if par2/par2create is missing, install it and re-run; \
with carb’s backfill, future runs will create parity for existing blobs."
fi

# --- 3) Prepare INCREMENTAL change & ref cutoff -------------------------------
sleep 1
echo "new line" >> data/docs/readme.txt    # Make readme newer than REF

# Make pic.bin older than REF (portable)
if touch -d "yesterday" data/media/pic.bin 2>/dev/null; then :; else touch -A -000100 data/media/pic.bin || true; fi

# Use the first run's start-time file as the cutoff reference (its mtime is the cutoff)
REF_FILE="$RUN_META/carb_starttime"

# --- 4) INCREMENTAL run (capture stderr) -------------------------------------
CARB_PAR2=1 ./carb.sh data "$REF_FILE" 2> incr_run.stderr || true

RUN_META2="$(ls -1d blobs_meta/v05_* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META2}" ]]; then
  echo "ERROR: No metadata directory after incremental run." >&2
  echo "---- carb INCREMENTAL run stderr (tail) ----"
  tail -n 200 incr_run.stderr || true
  exit 1
fi

echo "New blobs this run:"; cat "$RUN_META2/INDEX_NEW.txt" || true

# --- 5) Recovery demo (verify restore structure) ------------------------------
export CARB_RECOVER_TO_DIR="$WORKDIR/restore"
bash "$RUN_META2/recover.sh"

RESTORE_ROOT="$CARB_RECOVER_TO_DIR$PWD/data"
echo "Restored tree under: $RESTORE_ROOT"
find "$RESTORE_ROOT" -maxdepth 3 -type f -print || true

# --- 6) Targeted corruption of readme.txt & PAR2 repair proof -----------------
# Map readme.txt -> its blob from latest run’s file_processed.txt
VICTIM_LINE="$(grep -F -- "$WORKDIR/data/docs/readme.txt" "$RUN_META2/file_processed.txt" | tail -n1 || true)"
if [[ -z "$VICTIM_LINE" ]]; then
  echo "ERROR: Could not map readme.txt to a blob in $RUN_META2/file_processed.txt" >&2
  echo "---- helpful context ----"
  echo "Processed listing (tail):"; tail -n 50 "$RUN_META2/file_processed.txt" || true
  exit 1
fi
VICTIM_BLOB="${VICTIM_LINE%%:*}"

# Ensure parity exists for the victim blob; incremental run should have backfilled if missing.
if ! compgen -G "blobs_par2/${VICTIM_BLOB}.par2" >/dev/null; then
  echo "WARN: Parity for readme.txt blob not found; re-running once to backfill..." >&2
  CARB_PAR2=1 ./carb.sh data --full >/dev/null 2>&1 || true
fi

echo "Corrupting blobs_sha256/$VICTIM_BLOB (flip 1 byte @ offset 64)"
printf '\x00' | dd of="blobs_sha256/$VICTIM_BLOB" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

# Restore again to a fresh directory; recovery should verify/repair with PAR2
rm -rf "$WORKDIR/restore_repair"
export CARB_RECOVER_TO_DIR="$WORKDIR/restore_repair"
bash "$RUN_META2/recover.sh"

RESTORE_REPAIR_ROOT="$CARB_RECOVER_TO_DIR$PWD/data"
echo "After repair, restored tree under: $RESTORE_REPAIR_ROOT"
find "$RESTORE_REPAIR_ROOT" -maxdepth 3 -type f -print || true

# Now the comparison actually validates repair of the corrupted file
if diff -u "data/docs/readme.txt" "$RESTORE_REPAIR_ROOT/docs/readme.txt" >/dev/null; then
  echo "✅ readme.txt repaired (or verified clean) via PAR2."
else
  echo "❌ readme.txt differs. If parity is missing, install par2/par2create and re-run."
  echo "---- FULL run stderr (tail) ----"; tail -n 50 full_run.stderr || true
  echo "---- INCR run stderr (tail) ----"; tail -n 50 incr_run.stderr || true
  exit 2
fi

echo "✅ Demo complete."
echo "Workspace: $WORKDIR"