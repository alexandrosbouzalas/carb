#!/usr/bin/env bash
# Demo for carb: full + incremental backup and PAR2 repair.
# This version asks *the demo's own* "Install PAR2?" question first.
# If you answer N, it exits immediately. If Y, it auto-installs via carb.

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

# Show prompts on terminal and also capture stderr
run_with_visible_prompt_stderr() {
  local logfile="$1"; shift
  exec 3>&2
  # shellcheck disable=SC2068
  "$@" 2> >(tee -a "$logfile" >&3)
  local rc=$?
  exec 3>&-
  return $rc
}

# Read a Y/N answer from the *controlling TTY* even if stdin is redirected.
ask_yes_no() {
  local prompt="${1:-Proceed? [y/N] }"
  local ans=""
  if [[ -t 0 ]]; then
    read -r -p "$prompt" ans
  else
    if [[ -r /dev/tty ]]; then
      # shellcheck disable=SC2162
      read -r -p "$prompt" ans < /dev/tty
    else
      echo "Non-interactive session; defaulting to NO." >&2
      ans="n"
    fi
  fi
  [[ "$ans" =~ ^[Yy]$ ]]
}

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

# ---------- 1) Generate sample tree ----------
section "GENERATE SAMPLE TREE"
mkdir -p data/docs data/media data/tmp
printf "hello world\n" > data/docs/readme.txt
printf 'A%.0s' {1..1000} > data/docs/notes.txt
dd if=/dev/urandom of=data/media/pic.bin bs=1K count=64 status=none 2>/dev/null
printf "ignore me\n" > data/tmp/scratch.swp
good "Sample data created: data/{docs,media,tmp}"

# ---------- 1a) PAR2 preflight (our prompt; immediate abort on 'N') ----------
section "PAR2 PREFLIGHT"
PAR2_BIN="$(command -v par2 || true)"
PAR2_CREATE_BIN="$(command -v par2create || true)"
if [[ -z "$PAR2_BIN" && -z "$PAR2_CREATE_BIN" ]]; then
  warn "PAR2 is not installed. This demo relies on PAR2 to show repair."
  if ask_yes_no "Install PAR2 automatically when carb runs? [y/N] "; then
    DEMO_WANTS_PAR2="yes"
  else
    fail "You chose not to install PAR2. Aborting now to avoid a degraded demo."
    echo "Tip: re-run and answer 'y', or install 'par2'/'par2cmdline' manually first."
    exit 1
  fi
else
  DEMO_WANTS_PAR2="already"
  good "PAR2 detected."
fi

# ---------- 2) FULL run ----------
section "FULL RUN" "(exclude tmp-like files; capture stderr; prompts visible)"
if [[ "${DEMO_WANTS_PAR2:-}" == "yes" ]]; then
  # Pass env to *carb* by making them part of the command (via `env`)
  run_with_visible_prompt_stderr full_run.stderr \
    env CARB_EXCLUDE_GLOBS="*.swp,.DS_Store" \
        CARB_COMMENT="initial demo run" \
        CARB_PAR2=1 \
        CARB_AUTOINSTALL_ASK=1 \
        CARB_AUTOINSTALL_YES=y \
        ./carb.sh data --full || true
else
  run_with_visible_prompt_stderr full_run.stderr \
    env CARB_EXCLUDE_GLOBS="*.swp,.DS_Store" \
        CARB_COMMENT="initial demo run" \
        CARB_PAR2=1 \
        CARB_AUTOINSTALL_ASK=1 \
        CARB_AUTOINSTALL_YES= \
        ./carb.sh data --full || true
fi

# ---------- Find run metadata & effective CARB_HOME ----------
default_home_mac="${HOME}/Library/Application Support/carb"
default_home_linux="${XDG_DATA_HOME:-$HOME/.local/share}/carb"
CARB_HOME_GUESS="${CARB_HOME:-$default_home_mac}"
META_ROOT="${CARB_HOME_GUESS}/blobs_meta"
RUN_META="$(ls -1d "${META_ROOT}/v05_"* 2>/dev/null | tail -n1 || true)"
if [[ -z "${RUN_META}" ]]; then
  META_ROOT="${default_home_linux}/blobs_meta"
  RUN_META="$(ls -1d "${META_ROOT}/v05_"* 2>/dev/null | tail -n1 || true)"
fi
if [[ -z "${RUN_META}" ]]; then
  fail "No run metadata found; full run likely failed."
  echo; echo "---- carb FULL run stderr (tail) ----"
  tail -n 200 full_run.stderr || true
  exit 1
fi
printf "  Latest meta: %s\n" "$RUN_META"
printf "  carb_settings:\n"; sed -n '1p' "$RUN_META/carb_settings" || true; echo

CARB_HOME_USED="$(sed -n 's/.*home=\(.*\)$/\1/p' "$RUN_META/carb_settings" | head -n1)"
[[ -z "$CARB_HOME_USED" ]] && CARB_HOME_USED="${CARB_HOME_GUESS}"
BLOB_DIR="${CARB_HOME_USED}/blobs_sha256"
PAR_DIR="${CARB_HOME_USED}/blobs_par2"

# ---------- Require parity now (if we asked to install it) ----------
section "PARITY CHECK"
if [[ "${DEMO_WANTS_PAR2:-}" == "yes" || "${DEMO_WANTS_PAR2:-}" == "already" ]]; then
  if ! compgen -G "${PAR_DIR}/*.par2" >/dev/null; then
    fail "No PAR2 parity found after full run."
    echo "If you just installed PAR2, run a full backup again to generate parity."
    echo "Try: CARB_PAR2=1 ./carb.sh data --full"
    exit 1
  fi
  good "Parity detected — continuing."
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
section "INCREMENTAL RUN" "(capture stderr; prompts visible)"
if [[ "${DEMO_WANTS_PAR2:-}" == "yes" ]]; then
  run_with_visible_prompt_stderr incr_run.stderr \
    env CARB_PAR2=1 CARB_AUTOINSTALL_ASK=1 CARB_AUTOINSTALL_YES=y \
        ./carb.sh data "$REF_FILE" || true
else
  run_with_visible_prompt_stderr incr_run.stderr \
    env CARB_PAR2=1 CARB_AUTOINSTALL_ASK=1 CARB_AUTOINSTALL_YES= \
        ./carb.sh data "$REF_FILE" || true
fi

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
[[ -z "$CARB_START_BASENAME" ]] && CARB_START_BASENAME="data"

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
  if [[ "${DEMO_WANTS_PAR2:-}" == "yes" ]]; then
    run_with_visible_prompt_stderr backfill.stderr \
      env CARB_PAR2=1 CARB_AUTOINSTALL_ASK=1 CARB_AUTOINSTALL_YES=y \
          ./carb.sh data --full >/dev/null 2>&1 || true
  else
    run_with_visible_prompt_stderr backfill.stderr \
      env CARB_PAR2=1 CARB_AUTOINSTALL_ASK=1 CARB_AUTOINSTALL_YES= \
          ./carb.sh data --full >/dev/null 2>&1 || true
  fi
fi

info "Corrupting blob (flip 1 byte @ offset 64)"
printf '\x00' | dd of="${BLOB_DIR}/${VICTIM_BLOB}" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

rm -rf "$WORKDIR/restore_repair"
export CARB_RECOVER_TO_DIR="$WORKDIR/restore_repair"
bash "$RUN_META2/recover.sh" --damaged

info "Starting restore in 'damaged' mode..."

RESTORE_REPAIR_ROOT="$CARB_RECOVER_TO_DIR/$CARB_START_BASENAME"
info "After repair, restored tree under: ${RESTORE_REPAIR_ROOT}"
find "$RESTORE_REPAIR_ROOT" -maxdepth 3 -type f -print || true

if diff -u "data/docs/readme.txt" "$RESTORE_REPAIR_ROOT/docs/readme.txt" >/dev/null; then
  good "readme.txt repaired (or verified clean) via PAR2."
else
  fail "readme.txt differs. Inspect parity or rerun full backup to regenerate PAR2."
  echo "---- FULL run stderr (tail) ----"; tail -n 50 full_run.stderr || true
  echo "---- INCR run stderr (tail) ----"; tail -n 50 incr_run.stderr || true
  exit 2
fi

section "DEMO COMPLETE"
info "Workspace: ${BOLD}${WORKDIR}${RESET}"