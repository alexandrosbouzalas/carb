#!/usr/bin/env bash
set -Eeuo pipefail

# ---------- styling ----------
if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ $(tput colors 2>/dev/null || echo 0) -ge 8 ]]; then
  BOLD="$(tput bold)"; DIM="$(tput dim)"; RESET="$(tput sgr0)"
  RED="$(tput setaf 1)"; GREEN="$(tput setaf 2)"; YELLOW="$(tput setaf 3)"
  BLUE="$(tput setaf 4)"; MAGENTA="$(tput setaf 5)"; CYAN="$(tput setaf 6)"; GREY="$(tput setaf 7)"
else
  BOLD=""; DIM=""; RESET=""; RED=""; GREEN=""; YELLOW=""; BLUE=""; MAGENTA=""; CYAN=""; GREY=""
fi
OK="${GREEN}[OK]${RESET}"; WARN="${YELLOW}[WARN]${RESET}"; ERR="${RED}[ERR]${RESET}"
hr() { printf "%s\n" "${DIM}-------------------------------------------------------------------------------${RESET}"; }
section() { printf "\n%s%s%s %s\n" "${BOLD}" "${1}" "${RESET}" "${DIM}${2:-}${RESET}"; hr; }
info() { printf "%s %s\n" "${CYAN}[INFO]${RESET}" "$*"; }
note() { printf "%s %s\n" "${GREY}[....]${RESET}" "$*"; }
good() { printf "%s %s\n" "${OK}" "$*"; }
warn() { printf "%s %s\n" "${WARN}" "$*"; }
fail() { printf "%s %s\n" "${ERR}" "$*"; }
trap 'printf "\n"' EXIT

# ---------- 0) Clean playground ----------
WORKDIR="$(mktemp -d)"; cd "$WORKDIR"
section "DEMO WORKSPACE" "${WORKDIR}"
info "Workspace created at: ${BOLD}${WORKDIR}${RESET}"

# Locate carb and stage a local copy so outputs are self-contained
CARB_BIN="$(command -v carb || true)"
if [[ -z "$CARB_BIN" ]]; then
  fail "'carb' not found in PATH. Install it first, then re-run this demo."
  exit 1
fi
cp "$CARB_BIN" ./carb.sh
chmod +x ./carb.sh
good "carb staged locally: ./carb.sh"

# PAR2 availability (informational)
PAR2_BIN="$(command -v par2 || true)"
PAR2_CREATE_BIN="$(command -v par2create || true)"
info "PAR2: par2=${PAR2_BIN:-<missing>}  par2create=${PAR2_CREATE_BIN:-<missing>}"

# ---------- 1) Generate sample tree ----------
section "GENERATE SAMPLE TREE"
mkdir -p data/docs data/media data/tmp
printf "hello world\n" > data/docs/readme.txt
printf 'A%.0s' {1..1000} > data/docs/notes.txt
dd if=/dev/urandom of=data/media/pic.bin bs=1K count=64 status=none 2>/dev/null
printf "ignore me\n" > data/tmp/scratch.swp
good "Sample data created: data/{docs,media,tmp}"

# ---------- 2) FULL run ----------
section "FULL RUN" "(exclude tmp-like files; capture stderr)"
CARB_EXCLUDE_GLOBS="*.swp,.DS_Store" \
CARB_COMMENT="initial demo run" \
CARB_PAR2=1 \
./carb.sh data --full 2> full_run.stderr || true

note "Artifacts:"
echo "== blobs:"; ls -1 "blobs_sha256" 2>/dev/null | head || echo "(stored under CARB_HOME)"
echo

echo "== meta (latest):"
# Find latest run meta under effective CARB_HOME
default_home_mac="${HOME}/Library/Application Support/carb"
default_home_linux="${XDG_DATA_HOME:-$HOME/.local/share}/carb"
# Prefer env CARB_HOME if user set it, else mac default (we're on macOS in your logs), else linux default
CARB_HOME_GUESS="${CARB_HOME:-$default_home_mac}"
META_ROOT="${CARB_HOME_GUESS}/blobs_meta"
RUN_META="$(ls -1d "${META_ROOT}/v05_"* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META}" ]]; then
  # fall back to linux default if mac path was wrong
  META_ROOT="${default_home_linux}/blobs_meta"
  RUN_META="$(ls -1d "${META_ROOT}/v05_"* 2>/dev/null | tail -n1 || true)"
fi
if [[ -z "${RUN_META}" ]]; then
  fail "No run metadata found; full run likely failed."
  echo; echo "---- carb FULL run stderr (tail) ----"
  tail -n 200 full_run.stderr || true
  exit 1
fi
printf "  %s\n" "$RUN_META"
printf "  carb_settings:\n"
sed -n '1p' "$RUN_META/carb_settings" || true
echo

# Extract effective CARB_HOME actually used by the run
CARB_HOME_USED="$(sed -n 's/.*home=\(.*\)$/\1/p' "$RUN_META/carb_settings" | head -n1)"
if [[ -z "$CARB_HOME_USED" ]]; then CARB_HOME_USED="$CARB_HOME_GUESS"; fi
BLOB_DIR="${CARB_HOME_USED}/blobs_sha256"
PAR_DIR="${CARB_HOME_USED}/blobs_par2"

echo "== parity files:"
if compgen -G "${PAR_DIR}/*.par2" >/dev/null; then
  (cd "$PAR_DIR" && ls -1 | head) || true
else
  warn "(none yet) — if par2/par2create is missing, install it and re-run; carb will backfill on later runs."
fi

# ---------- 3) Prepare INCREMENTAL change & ref cutoff ----------
section "PREPARE INCREMENTAL CHANGE & REF CUTOFF"
sleep 1
echo "new line" >> data/docs/readme.txt
note "readme.txt updated to be newer than REF"

if touch -d "yesterday" data/media/pic.bin 2>/dev/null; then
  note "pic.bin timestamp set to yesterday"
else
  touch -A -000100 data/media/pic.bin || true
  note "pic.bin timestamp nudged older via touch -A"
fi

REF_FILE="$RUN_META/carb_starttime"
info "Reference cutoff set to: ${REF_FILE}"

# ---------- 4) INCREMENTAL run ----------
section "INCREMENTAL RUN" "(capture stderr)"
CARB_PAR2=1 ./carb.sh data "$REF_FILE" 2> incr_run.stderr || true

# Refresh latest meta after incremental
RUN_META2="$(ls -1d "${META_ROOT}/v05_"* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META2}" ]]; then
  fail "No metadata directory after incremental run."
  echo; echo "---- carb INCREMENTAL run stderr (tail) ----"
  tail -n 200 incr_run.stderr || true
  exit 1
fi

echo "New blobs this run:"; cat "$RUN_META2/INDEX_NEW.txt" || true

# Parse CARB_START_BASENAME from the run's recover.sh so we restore to the right place
CARB_START_BASENAME="$(sed -n 's/^CARB_START_BASENAME="\([^"]*\)".*/\1/p' "$RUN_META2/recover.sh" | head -n1)"
if [[ -z "$CARB_START_BASENAME" ]]; then CARB_START_BASENAME="data"; fi

# ---------- 5) Recovery demo ----------
section "RECOVERY DEMO" "(verify restore structure)"
export CARB_RECOVER_TO_DIR="$WORKDIR/restore"
bash "$RUN_META2/recover.sh"

RESTORE_ROOT="$CARB_RECOVER_TO_DIR/$CARB_START_BASENAME"
info "Restored tree under: ${RESTORE_ROOT}"
find "$RESTORE_ROOT" -maxdepth 3 -type f -print || true

# ---------- 6) Targeted corruption & PAR2 repair ----------
section "PAR2 REPAIR PROOF" "(corrupt readme blob then recover)"
VICTIM_LINE="$(grep -F -- "$WORKDIR/data/docs/readme.txt" "$RUN_META2/file_processed.txt" | tail -n1 || true)"
if [[ -z "$VICTIM_LINE" ]]; then
  fail "Could not map readme.txt to a blob in $RUN_META2/file_processed.txt"
  echo "---- helpful context ----"; tail -n 50 "$RUN_META2/file_processed.txt" || true
  exit 1
fi
VICTIM_BLOB="${VICTIM_LINE%%:*}"
info "Victim blob: ${VICTIM_BLOB}"

# ensure parity exists/backfill
if ! compgen -G "${PAR_DIR}/${VICTIM_BLOB}.par2" >/dev/null; then
  warn "Parity for readme.txt blob not found; re-running once to backfill..."
  CARB_PAR2=1 ./carb.sh data --full >/dev/null 2>&1 || true
fi

info "Corrupting blob (flip 1 byte @ offset 64)"
printf '\x00' | dd of="${BLOB_DIR}/${VICTIM_BLOB}" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

rm -rf "$WORKDIR/restore_repair"
export CARB_RECOVER_TO_DIR="$WORKDIR/restore_repair"
bash "$RUN_META2/recover.sh"

RESTORE_REPAIR_ROOT="$CARB_RECOVER_TO_DIR/$CARB_START_BASENAME"
info "After repair, restored tree under: ${RESTORE_REPAIR_ROOT}"
find "$RESTORE_REPAIR_ROOT" -maxdepth 3 -type f -print || true

if diff -u "data/docs/readme.txt" "$RESTORE_REPAIR_ROOT/docs/readme.txt" >/dev/null; then
  good "readme.txt repaired (or verified clean) via PAR2."
else
  fail "readme.txt differs. If parity is missing, install par2/par2create and re-run."
  echo "---- FULL run stderr (tail) ----"; tail -n 50 full_run.stderr || true
  echo "---- INCR run stderr (tail) ----"; tail -n 50 incr_run.stderr || true
  exit 2
fi

section "DEMO COMPLETE"
info "Workspace: ${BOLD}${WORKDIR}${RESET}"