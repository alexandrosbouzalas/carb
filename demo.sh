#!/usr/bin/env bash
set -Eeuo pipefail

# 0) Clean playground
WORKDIR="$(mktemp -d)"; cd "$WORKDIR"
echo "Demo workspace: $WORKDIR"

CARB_SRC="${CARB_SRC:-}"
if [[ -z "${CARB_SRC}" ]]; then
  if [[ -r /opt/carb/carb.bash ]]; then
    CARB_SRC=/opt/carb/carb.bash
  elif [[ -r ./carb.bash ]]; then
    CARB_SRC=./carb.bash
  else
    echo "ERROR: Could not find carb.bash. Set \$CARB_SRC or install at /opt/carb/carb.bash (or put carb.bash next to this demo)." >&2
    exit 1
  fi
fi

cp "$CARB_SRC" ./carb.bash
chmod +x ./carb.bash

# Check PAR2 availability
PAR2_BIN="$(command -v par2 || true)"
PAR2_CREATE_BIN="$(command -v par2create || true)"
echo "PAR2: par2=${PAR2_BIN:-<missing>} par2create=${PAR2_CREATE_BIN:-<missing>}"

# Generate Sample tree
mkdir -p data/docs data/media data/tmp
echo "hello world" > data/docs/readme.txt
printf 'A%.0s' {1..1000} > data/docs/notes.txt      # ~1 KiB
dd if=/dev/urandom of=data/media/pic.bin bs=1K count=64 2>/dev/null
echo "ignore me" > data/tmp/scratch.swp

# First run: FULL backup (exclude tmp-ish files); parity will be created if par2 is available
CARB_EXCLUDE_GLOBS="*.swp,.DS_Store" \
CARB_COMMENT="initial demo run" \
CARB_PAR2=1 \
./carb.bash data --full 2> full_run.stderr || true

# Show basic artifacts
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

# verify parity exists (or explain why not)
echo "== parity files:"
if compgen -G "blobs_par2/*.par2" >/dev/null; then
  ls -1 blobs_par2 | head || true
else
  echo "(none yet) — if par2/par2create is missing, install it and re-run; \
with the updated carb.bash, future runs will backfill parity for existing blobs."
fi

# Simulate change for INCREMENTAL
sleep 1
echo "new line" >> data/docs/readme.txt
# Make pic.bin older than ref (portable)
if touch -d "yesterday" data/media/pic.bin 2>/dev/null; then :; else touch -A -000100 data/media/pic.bin; fi
REF_FILE="$RUN_META/carb_starttime"

# Second run: INCREMENTAL backup (also backfills parity for any skipped blobs missing .par2)
CARB_PAR2=1 ./carb.bash data "$REF_FILE" 2> /dev/null || true

RUN_META2="$(ls -1d blobs_meta/v05_* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META2}" ]]; then
  echo "ERROR: No metadata directory after incremental run." >&2
  exit 1
fi
echo "New blobs this run:"; cat "$RUN_META2/INDEX_NEW.txt" || true

# Recovery demo (from latest run)
export CARB_RECOVER_TO_DIR="$WORKDIR/restore"
bash "$RUN_META2/recover.sh"

RESTORE_ROOT="$CARB_RECOVER_TO_DIR$PWD/data"
echo "Restored tree under: $RESTORE_ROOT"
find "$RESTORE_ROOT" -type f -maxdepth 3 -print || true

# Corruption + repair showcase
# Prefer a newly ingested blob; if none, fall back to notes.txt blob from FULL run.
VICTIM_BLOB="$(head -n1 "$RUN_META2/INDEX_NEW.txt" 2>/dev/null || true)"
if [[ -z "${VICTIM_BLOB}" ]]; then
  VICTIM_BLOB="$(grep -F -- 'data/docs/notes.txt' "$RUN_META/file_processed.txt" 2>/dev/null | awk -F: '{print $1}' | head -n1 || true)"
fi
if [[ -z "${VICTIM_BLOB}" ]]; then
  echo "ERROR: Could not locate a blob to corrupt (INDEX_NEW empty and notes.txt not found)." >&2
  exit 1
fi

echo "Corrupting blobs_sha256/$VICTIM_BLOB (flip 1 byte @ offset 64)"
printf '\x00' | dd of="blobs_sha256/$VICTIM_BLOB" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

rm -rf "$WORKDIR/restore"
export CARB_RECOVER_TO_DIR="$WORKDIR/restore_repair"
bash "$RUN_META2/recover.sh"

RESTORE_REPAIR_ROOT="$CARB_RECOVER_TO_DIR$PWD/data"
echo "After repair, restored tree under: $RESTORE_REPAIR_ROOT"
find "$RESTORE_REPAIR_ROOT" -type f -maxdepth 3 -print || true

# Verify repaired file matches original readme; it changed after FULL run,
# so we compare the *current* source to its restored counterpart.
if diff -u "data/docs/readme.txt" "$RESTORE_REPAIR_ROOT/docs/readme.txt" >/dev/null; then
  echo "✅ readme.txt repaired via PAR2 (or verified clean)."
else
  echo "❌ readme.txt differs. If parity is missing, ensure par2/par2create is installed and re-run."
  if [[ -n "${PAR2_BIN}" ]]; then
    NOTES_BLOB="$(grep -F -- 'data/docs/notes.txt' "$RUN_META/file_processed.txt" 2>/dev/null | awk -F: '{print $1}' | head -n1 || true)"
    if [[ -n "${NOTES_BLOB}" ]]; then
      echo "---- manual par2 verify (notes.txt) ----"
      "$PAR2_BIN" verify -B / "blobs_par2/${NOTES_BLOB}.par2" -- "blobs_sha256/${NOTES_BLOB}" || true
    fi
  fi
  exit 2
fi