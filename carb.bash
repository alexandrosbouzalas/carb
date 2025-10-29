#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# carb — Content-addressable robust backup + per-file ingester with PAR2
#
# WHAT IT DOES
#   - Walks a start directory and ingests every regular file as a content-
#     addressed "blob" named "<size>_<sha256>.data" under ./blobs_sha256.
#   - Creates parity files (PAR2) per blob (configurable), enabling later
#     verification/repair on restore.
#   - Produces per-run metadata under ./blobs_meta/v05_<timestamp>, including:
#       * file_processed/skipped/ingested lists (with original absolute paths)
#       * basic stat captures and MIME types (optional)
#       * a ready-to-run recovery script (recover.sh)
#   - Maintains a global ./blobs_sha256/INDEX.txt of blob names (append-only).
#
# HOW TO RUN
#   Full backup:
#       carb <START_DIR>
#       carb <START_DIR> --full
#   Incremental (ingest only files newer than a reference file’s mtime):
#       carb <START_DIR> <REFERENCE_FILE>
#
# KEY BEHAVIOR / NOTES
#   - Safe across Linux and macOS. Uses GNU or BSD variants of stat/date where
#     needed. Auto-detects CPU count for parallel ingestion.
#   - Robust PAR2 detection (no brittle glob-with-ls). Adaptive blocksize/
#     redundancy by default; can be overridden with env vars.
#   - Worker subshells inherit strict flags (-Eeuo pipefail).
#   - Does NOT truncate the global INDEX on each run; it’s append-only.
#
# IMPORTANT ENVs (override as needed)
#   CARB_JOBS=<int>                # parallel workers (default: CPU count)
#   CARB_PAR2=1|0                  # enable/disable PAR2 (default: 1)
#   CARB_PAR2_REDUNDANCY=<int>     # % parity (default: 10)
#   CARB_PAR2_BLOCKSIZE=<int|auto> # block bytes or "auto" (default: auto)
#   CARB_PAR2_CMD=<cmd>            # par2 binary name (default: par2)
#   CARB_ENABLE_MIME=1|0           # run 'file' for MIME (default: 1)
#   CARB_EXCLUDE_GLOBS="*.tmp,..." # comma-separated glob patterns to prune
#   CARB_TMPDIR=<path>             # tmp dir (default: ./blobs_tmp)
#   CARB_AUTOINSTALL_ASK=1|0       # ask to auto-install deps (default: 1)
#   CARB_AUTOINSTALL_YES=1         # auto-confirm install (default: unset)
#   CARB_PKG_MANAGER=<mgr>         # force package manager (apt, dnf, brew, …)
#   CARB_COMMENT="text"            # annotated into run metadata
# -----------------------------------------------------------------------------

set -Eeuo pipefail
IFS=$'\n\t'

abort() { echo "ERROR: $*" >&2; exit 1; }
trap 'abort "line $LINENO exited with status $?"' ERR

have() { command -v "$1" >/dev/null 2>&1; }
is_tty() { [[ -t 0 && -t 1 ]]; }

# ---- Args & mode detection ---------------------------------------------------
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage:" >&2
  echo "  Incremental:  $0 <START_DIR> <REFERENCE_FILE>" >&2
  echo "  Full backup:  $0 <START_DIR>  (or: $0 <START_DIR> --full)" >&2
  exit 64
fi

START_ARG="$1"
REF_OR_FLAG="${2-}"

startdir_from_arg() {
  local p="$1" d
  d=$(dirname -- "${p%/}/__SENTRY__")
  [[ "$d" != /* ]] && d="$(pwd)/$d"
  printf '%s\n' "$d"
}
CARB_STARTDIR="$(startdir_from_arg "$START_ARG")"
[[ -d "$CARB_STARTDIR" ]] || abort "Start directory does not exist: $CARB_STARTDIR"

MODE="incremental"
REF_FILE=""
if [[ $# -eq 1 || "$REF_OR_FLAG" == "--full" ]]; then
  MODE="full"
else
  REF_FILE="$REF_OR_FLAG"
  [[ -f "$REF_FILE" ]] || abort "Reference file does not exist: $REF_FILE"
fi

# ---- Paths & run variables ---------------------------------------------------
TODAY=$(date "+%Y-%m-%d") || true
STARTTIME=$(date "+%Y-%m-%d_%H_%M_%S") || true
CARB_COMMENT="${CARB_COMMENT:-}"

_resolve() { command -v readlink >/dev/null 2>&1 && readlink -f -- "$1" || python3 - "$1" <<'PY' || echo "$1"
import os,sys;p=sys.argv[1];print(os.path.abspath(os.path.realpath(p)))
PY
}
SCRIPT_PATH="$(_resolve "${BASH_SOURCE[0]}")"
SCRIPT_BASE="$(dirname "$SCRIPT_PATH")"

DIR_BLOBS="${SCRIPT_BASE}/blobs_sha256"
DIR_TMP="${CARB_TMPDIR:-${SCRIPT_BASE}/blobs_tmp}"
DIR_META_ROOT="${SCRIPT_BASE}/blobs_meta"
DIR_META_RUN="${DIR_META_ROOT}/v05_${STARTTIME}"
DIR_PAR2="${SCRIPT_BASE}/blobs_par2"

PWD_AT_START=$(pwd)

detect_cpus() {
  if have nproc; then nproc
  elif [[ "$(uname -s)" == "Darwin" ]]; then sysctl -n hw.ncpu
  else echo 1
  fi
}
CARB_JOBS="${CARB_JOBS:-$(detect_cpus)}"
[[ "$CARB_JOBS" =~ ^[1-9][0-9]*$ ]] || CARB_JOBS=1

CARB_PAR2="${CARB_PAR2:-1}"
CARB_PAR2_REDUNDANCY="${CARB_PAR2_REDUNDANCY:-10}"
CARB_PAR2_BLOCKSIZE="${CARB_PAR2_BLOCKSIZE:-}"   # "" or "auto" => adaptive
CARB_PAR2_CMD="${CARB_PAR2_CMD:-par2}"
CARB_ENABLE_MIME="${CARB_ENABLE_MIME:-1}"
CARB_EXCLUDE_GLOBS="${CARB_EXCLUDE_GLOBS:-}"
CARB_AUTOINSTALL_ASK="${CARB_AUTOINSTALL_ASK:-1}"
CARB_AUTOINSTALL_YES="${CARB_AUTOINSTALL_YES:-}"
CARB_PKG_MANAGER="${CARB_PKG_MANAGER:-}"

# ---- Dependency check --------------------------------------------------------
dep_check_and_maybe_install() {
  local -a missing_req=() missing_opt=() req=(find xargs awk sed tee mktemp ln cp stat date)
  local c
  for c in "${req[@]}"; do have "$c" || missing_req+=("$c"); done
  if ! have openssl && ! have shasum; then missing_req+=("openssl (or shasum)"); fi
  [[ "$CARB_ENABLE_MIME" == "1" && ! $(command -v file) ]] && missing_opt+=("file")
  if [[ "$CARB_PAR2" == "1" ]] && ! have "$CARB_PAR2_CMD" && ! have par2create && ! have par2; then
    missing_req+=("par2cmdline")
  fi
  (( ${#missing_req[@]} == 0 && ${#missing_opt[@]} == 0 )) && return 0
  echo "==> Pre-flight dependency check" >&2
  (( ${#missing_req[@]} )) && { echo "Missing REQUIRED tools:" >&2; printf '  - %s\n' "${missing_req[@]}" >&2; }
  (( ${#missing_opt[@]} )) && { echo "Missing OPTIONAL tools:" >&2; printf '  - %s\n' "${missing_opt[@]}" >&2; }

  local mgr="${CARB_PKG_MANAGER}"
  if [[ -z "$mgr" ]]; then
    if   have apt-get; then mgr="apt"
    elif have dnf;     then mgr="dnf"
    elif have yum;     then mgr="yum"
    elif have pacman;  then mgr="pacman"
    elif have zypper;  then mgr="zypper"
    elif have apk;     then mgr="apk"
    elif have brew;    then mgr="brew"
    elif have port;    then mgr="port"
    else mgr=""
    fi
  fi

  local -a install_pkgs=()
  if ! have openssl && ! have shasum; then case "$mgr" in apt|dnf|yum|zypper|apk|pacman|brew|port) install_pkgs+=("openssl");; esac; fi
  if [[ "$CARB_ENABLE_MIME" == "1" ]] && ! have file; then case "$mgr" in apt|dnf|yum|zypper|apk|pacman|port) install_pkgs+=("file");; brew) install_pkgs+=("file");; esac; fi
  if [[ "$CARB_PAR2" == "1" ]] && ! have "$CARB_PAR2_CMD" && ! have par2create && ! have par2; then case "$mgr" in apt|zypper|apk|brew|port) install_pkgs+=("par2");; dnf|yum|pacman) install_pkgs+=("par2cmdline");; esac; fi

  if (( ${#install_pkgs[@]} )); then
    local do_install=""
    if [[ "${CARB_AUTOINSTALL_ASK}" == "0" ]]; then do_install="no"
    else do_install="${CARB_AUTOINSTALL_YES:-}"
         if [[ -z "$do_install" ]] && is_tty; then
           echo -n "Attempt to install missing packages (${install_pkgs[*]}) via ${mgr:-<unknown>}? [y/N] " >&2; read -r do_install || true
         fi
    fi
    if [[ "$do_install" == "1" || "$do_install" =~ ^[Yy]$ ]]; then
      local sudo=""; [[ "${EUID:-$(id -u)}" -ne 0 && "$(command -v sudo || true)" ]] && sudo="sudo "
      local cmd=""
      case "$mgr" in
        apt)    cmd="${sudo}apt-get update && ${sudo}apt-get install -y ${install_pkgs[*]}";;
        dnf)    cmd="${sudo}dnf install -y ${install_pkgs[*]}";;
        yum)    cmd="${sudo}yum install -y ${install_pkgs[*]}";;
        pacman) cmd="${sudo}pacman -Sy --noconfirm ${install_pkgs[*]}";;
        zypper) cmd="${sudo}zypper install -y ${install_pkgs[*]}";;
        apk)    cmd="${sudo}apk add --no-progress ${install_pkgs[*]}";;
        brew)   cmd="brew install ${install_pkgs[*]}";;
        port)   cmd="${sudo}port install ${install_pkgs[*]}";;
      esac
      [[ -n "$cmd" ]] && { echo "Running: $cmd" >&2; bash -c "$cmd" || abort "Automatic installation failed. Please install: ${install_pkgs[*]}"; } || echo "No supported package manager detected." >&2
    fi
  fi

  local -a still_missing=()
  for c in "${req[@]}"; do have "$c" || still_missing+=("$c"); done
  if ! have openssl && ! have shasum; then still_missing+=("openssl (or shasum)"); fi
  if [[ "$CARB_PAR2" == "1" ]] && ! have "$CARB_PAR2_CMD" && ! have par2create && ! have par2; then still_missing+=("par2cmdline"); fi
  if (( ${#still_missing[@]} )); then
    echo "Missing REQUIRED tools after attempted installation:" >&2
    printf '  - %s\n' "${still_missing[@]}" >&2
    echo "Please install the above and re-run. Aborting." >&2
    exit 69
  fi
  if [[ "$CARB_ENABLE_MIME" == "1" ]] && ! have file; then echo "WARN: 'file' not found; MIME detection will be skipped." >&2; fi
}
dep_check_and_maybe_install

mkdir -p -- "$DIR_BLOBS" "$DIR_TMP" "$DIR_META_ROOT" "$DIR_META_RUN" "$DIR_PAR2" "$DIR_META_RUN/logs"

: > "${DIR_META_RUN}/file_processed.txt"
: > "${DIR_META_RUN}/file_skipped.txt"
: > "${DIR_META_RUN}/file_ingested.txt"
touch "${DIR_BLOBS}/INDEX.txt"

RECOVER_SH="${DIR_META_RUN}/recover.sh"
printf '#!/usr/bin/env bash\nset -Eeuo pipefail\n: "${CARB_RECOVER_TO_DIR:?set CARB_RECOVER_TO_DIR to a target directory}"\n' > "$RECOVER_SH"
chmod +x "$RECOVER_SH"

printf '%s\n' "$STARTTIME"                          >> "${DIR_META_RUN}/carb_starttime"
printf 'pwd=%s CARB_STARTDIR=%s\n' "$PWD_AT_START" "$CARB_STARTDIR" > "${DIR_META_RUN}/carb_startfolder"

# Compose mode description early to avoid in-place sed edits later
MODE_DESC="$MODE"
TMP_REF=""
if [[ "$MODE" == "incremental" ]]; then
  REF_EPOCH="$( (stat -c %Y -- "$REF_FILE" 2>/dev/null || stat -f %m -- "$REF_FILE") )"
  REF_STR="$( (date -d @"$REF_EPOCH" "+%Y-%m-%d_%H_%M_%S" 2>/dev/null || date -r "$REF_EPOCH" "+%Y-%m-%d_%H_%M_%S") )"
  MODE_DESC="incremental ref=${REF_STR}"
  TMP_REF="$(mktemp "${DIR_TMP}/ref_${STARTTIME}.XXXX")"; touch -r "$REF_FILE" "$TMP_REF"
  echo "NEWER $REF_EPOCH $REF_STR"
else
  MODE_DESC="full"
  echo "FULL backup mode (no reference cutoff)"
fi

printf 'mode=%s par2=%s r=%s s=%s cmd=%s jobs=%s\n' \
  "$MODE_DESC" "$CARB_PAR2" "$CARB_PAR2_REDUNDANCY" "${CARB_PAR2_BLOCKSIZE:-auto}" "$CARB_PAR2_CMD" "$CARB_JOBS" \
  > "${DIR_META_RUN}/carb_settings"
printf '%s :%s:%s: %s : %s\n' "$STARTTIME" "$PWD_AT_START" "$CARB_STARTDIR" "$CARB_COMMENT" "$MODE_DESC" \
  >> "${DIR_META_ROOT}/ingestedFolders.txt"

# ---- Portable helpers --------------------------------------------------------
stat_epoch_mtime() { if stat -c %Y -- "$1" >/dev/null 2>&1; then stat -c %Y -- "$1"; else stat -f %m -- "$1"; fi; }
stat_filesize()    { if stat -c %s -- "$1" >/dev/null 2>&1; then stat -c %s -- "$1"; else stat -f %z -- "$1"; fi; }
date_from_epoch()  { local e="$1"; date -d @"$e" "+%Y-%m-%d_%H_%M_%S" 2>/dev/null || date -r "$e" "+%Y-%m-%d_%H_%M_%S"; }

hash_stream_sha256() {
  if have openssl; then openssl dgst -sha256 - 2>/dev/null | awk '{print $NF}'
  elif have shasum; then shasum -a 256 | awk '{print $1}'
  else echo "FATAL: need openssl or shasum for SHA-256" >&2; return 127
  fi
}

# ---- Recovery helpers (embedded) --------------------------------------------
{
cat <<'REC'
_have_par2_files() {
  local base="$1"
  [[ -e "${base}.par2" ]] && return 0
  local m; m=$(compgen -G "${base}".vol*.par2 2>/dev/null || true)
  [[ -n "$m" ]]
}

_select_par2_cmd() {
  local want="${CARB_PAR2_CMD:-}"
  if [[ -n "$want" ]] && command -v "$want" >/dev/null 2>&1; then printf '%s\n' "$want"; return 0; fi
  if command -v par2create >/dev/null 2>&1; then printf 'par2create\n'; return 0; fi
  if command -v par2 >/dev/null 2>&1; then printf 'par2\n'; return 0; fi
  printf '\n'
}

par2_verify_or_repair() {
  local src="$1" dest="$2" pardir="$3" blobname="$4" prefer="$5"
  local base="${pardir}/${blobname}"
  local cmd="$prefer"; [[ -n "$cmd" ]] || cmd="$(_select_par2_cmd)"

  if ! _have_par2_files "$base"; then
    cp -- "$src" "$dest"; return
  fi

  if [[ -n "$cmd" ]] && command -v "$cmd" >/dev/null 2>&1; then
    if "$cmd" verify -q -B / "${base}.par2" -- "$src" >/dev/null 2>&1; then
      cp -- "$src" "$dest"; return
    fi
    if "$cmd" repair -q -B / "${base}.par2" -- "$src" >/dev/null 2>&1; then
      "$cmd" verify -q -B / "${base}.par2" -- "$src" >/dev/null 2>&1 || true
      cp -- "$src" "$dest"; return
    fi
    echo "WARN: PAR2 repair failed for $src; copying original bytes" >&2
    cp -- "$src" "$dest"
  else
    echo "WARN: par2 not found during recovery; copying without verify" >&2
    cp -- "$src" "$dest"
  fi
}
REC
} >> "$RECOVER_SH"

# ---- Adaptive PAR2 helpers ---------------------------------------------------
_next_pow2() { local n="$1"; (( n < 1 )) && { echo 1; return; }; local p=1; while (( p < n )); do (( p <<= 1 )); done; echo "$p"; }

_par2_plan_for_size() {
  local sz="$1"
  if [[ -n "${CARB_PAR2_BLOCKSIZE:-}" && "${CARB_PAR2_BLOCKSIZE:-}" != "auto" && -n "${CARB_PAR2_REDUNDANCY:-}" ]]; then
    echo "$CARB_PAR2_BLOCKSIZE $CARB_PAR2_REDUNDANCY"; return
  fi
  local TARGET_DATA_SLICES=16 MIN_PARITY_SLICES=4 MIN_BLOCK=512 MAX_BLOCK=$((4*1024*1024)) DEFAULT_R="${CARB_PAR2_REDUNDANCY:-10}"

  if [[ -n "${CARB_PAR2_BLOCKSIZE:-}" && "${CARB_PAR2_BLOCKSIZE:-}" != "auto" ]]; then
    local bs="${CARB_PAR2_BLOCKSIZE}" ds=$(( (sz + bs - 1) / bs )); (( ds < 1 )) && ds=1
    local r="${DEFAULT_R}" ps=$(( (ds * r + 99) / 100 ))
    if (( ps < MIN_PARITY_SLICES )); then r=$(( (MIN_PARITY_SLICES * 100 + ds - 1) / ds )); (( r > 80 )) && r=80; fi
    echo "$bs $r"; return
  fi

  local bs=$(( sz / TARGET_DATA_SLICES )); (( bs < MIN_BLOCK )) && bs="$MIN_BLOCK"
  bs="$(_next_pow2 "$bs")"; (( bs > MAX_BLOCK )) && bs="$MAX_BLOCK"; (( bs < 1 )) && bs=1
  local ds=$(( (sz + bs - 1) / bs )); (( ds < 1 )) && ds=1
  local r="${DEFAULT_R}" ps=$(( (ds * r + 99) / 100 ))
  if (( ps < MIN_PARITY_SLICES )); then r=$(( (MIN_PARITY_SLICES * 100 + ds - 1) / ds )); (( r > 80 )) && r=80; fi
  echo "$bs $r"
}
export -f _next_pow2 _par2_plan_for_size

# ---- PAR2 create helper (race-safe) -----------------------------------------
_have_par2_files() {
  local base="$1"
  [[ -e "${base}.par2" ]] && return 0
  local m; m=$(compgen -G "${base}".vol*.par2 2>/dev/null || true)
  [[ -n "$m" ]]
}

par2_create_for_blob() {
  [[ "$CARB_PAR2" == "1" ]] || return 0
  local blobpath="$1" blobname="$2" base="${DIR_PAR2}/${blobname}"
  local cmd="$CARB_PAR2_CMD"

  if ! command -v "$cmd" >/dev/null 2>&1; then
    if command -v par2create >/dev/null 2>&1; then cmd=par2create
    elif command -v par2 >/dev/null 2>&1; then cmd=par2
    else
      echo "WARN: PAR2 requested but no par2/par2create found" >&2; return 0
    fi
  fi
  if _have_par2_files "$base"; then return 0; fi

  local lock="${DIR_PAR2}/.lock_${blobname}"
  if ! mkdir "$lock" 2>/dev/null; then
    local tries=50; while (( tries-- > 0 )) && [[ ! -e "${base}.par2" ]]; do sleep 0.1; done
    return 0
  fi

  local bs_opt="" r_opt="" s_bytes="" r_pct=""
  if [[ -z "${CARB_PAR2_BLOCKSIZE:-}" || "${CARB_PAR2_BLOCKSIZE:-}" == "auto" ]]; then
    local fsz="${blobname%%_*}"; fsz="${fsz#0}"; [[ -z "$fsz" ]] && fsz=0
    read -r s_bytes r_pct < <(_par2_plan_for_size "$fsz")
    bs_opt="-s${s_bytes}"; r_opt="-r${r_pct}"
  else
    [[ -n "${CARB_PAR2_BLOCKSIZE:-}"  ]] && bs_opt="-s${CARB_PAR2_BLOCKSIZE}"
    local r_use="${CARB_PAR2_REDUNDANCY:-10}"; r_opt="-r${r_use}"
  fi

  local args=(-q -B /); [[ -n "$r_opt" ]] && args+=("$r_opt"); [[ -n "$bs_opt" ]] && args+=("$bs_opt")
  if "$cmd" create "${args[@]}" "${base}.par2" -- "$blobpath" 2>>"${DIR_META_RUN}/par2.stderr"; then
    printf '%s\n' "${blobname}" >> "${DIR_META_RUN}/par2_created.txt"
  else
    echo "WARN: PAR2 creation failed for ${blobname}" >&2
  fi
  rmdir "$lock" 2>/dev/null || true
}
export -f par2_create_for_blob _have_par2_files

# ---- Per-file ingest worker --------------------------------------------------
ingest_one() {
  local src="$1"; [[ -f "$src" ]] || return 0

  local wid="$$" LOGDIR="${DIR_META_RUN}/logs"
  local LOG_PROC="${LOGDIR}/${wid}_processed.txt"
  local LOG_SKIP="${LOGDIR}/${wid}_skipped.txt"
  local LOG_INGE="${LOGDIR}/${wid}_ingested.txt"
  local LOG_STAT1="${LOGDIR}/${wid}_stat1.txt"
  local LOG_STAT2="${LOGDIR}/${wid}_stat2.txt"
  local LOG_TYPES="${LOGDIR}/${wid}_types.csv"
  local LOG_RECOV="${LOGDIR}/${wid}_recover.sh"
  : > "$LOG_PROC" 2>/dev/null || true
  : > "$LOG_SKIP" 2>/dev/null || true
  : > "$LOG_INGE" 2>/dev/null || true
  : > "$LOG_STAT1" 2>/dev/null || true
  : > "$LOG_STAT2" 2>/dev/null || true
  : > "$LOG_TYPES" 2>/dev/null || true
  : > "$LOG_RECOV" 2>/dev/null || true

  local abs="$src"
  if [[ "$abs" != /* ]]; then abs="$(cd "$(dirname -- "$src")" && pwd)/$(basename -- "$src")"; fi

  local size; size="$(stat_filesize "$src")" || return 1
  printf -v size "%018d" "$size"

  local tmpcopy; tmpcopy="$(mktemp "${DIR_TMP}/${TODAY}.XXXX")"

  local hash
  if ! hash="$(tee -- "$tmpcopy" < "$src" | hash_stream_sha256)"; then
    rm -f -- "$tmpcopy"; abort "sha256 failed for $src"
  fi

  local blobname="${size}_${hash}.data" blobpath="${DIR_BLOBS}/${blobname}"

  printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_PROC"

  if stat -s "$src" >/dev/null 2>&1; then
    printf '%s %s\n' "$blobname" "$(stat -s "$src")" >> "$LOG_STAT1"
    stat -x "$src" 2>/dev/null | sed "s#^#${blobname} #" >> "$LOG_STAT2" || true
  else
    printf '%s size=%s mode=%s uid=%s gid=%s mtime=%s\n' \
      "$blobname" "$(stat -c %s -- "$src")" "$(stat -c %a -- "$src")" \
      "$(stat -c %u -- "$src")" "$(stat -c %g -- "$src")" "$(stat -c %Y -- "$src")" >> "$LOG_STAT1"
    stat --printf='%n: %A %h %U %G %s %y\n' -- "$src" | sed "s#^#${blobname} #" >> "$LOG_STAT2"
  fi

  if ln "$tmpcopy" "$blobpath" 2>/dev/null; then
    printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_INGE"
    rm -f -- "$tmpcopy"
    if [[ "$CARB_ENABLE_MIME" == "1" ]] && have file; then
      local mt; mt="$(file -b --mime-type "$src" 2>/dev/null || file -b --mime "$src" 2>/dev/null || true)"
      printf '"%s", "%s"\n' "$blobname" "$mt" >> "$LOG_TYPES"
    fi
    par2_create_for_blob "$blobpath" "$blobname"
  else
    if [[ -e "$blobpath" ]]; then
      printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_SKIP"
      rm -f -- "$tmpcopy"
      if [[ "$CARB_PAR2" == "1" ]] && ! _have_par2_files "${DIR_PAR2}/${blobname}"; then
        par2_create_for_blob "$blobpath" "$blobname"
      fi
    else
      if mv -n -- "$tmpcopy" "$blobpath" 2>/dev/null; then
        printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_INGE"
      elif cp -n -- "$tmpcopy" "$blobpath" 2>/dev/null; then
        rm -f -- "$tmpcopy"
        printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_INGE"
      else
        rm -f -- "$tmpcopy"; abort "Failed to install blob $blobname to $DIR_BLOBS"
      fi
      if [[ "$CARB_ENABLE_MIME" == "1" ]] && have file; then
        local mt; mt="$(file -b --mime-type "$src" 2>/dev/null || file -b --mime "$src" 2>/dev/null || true)"
        printf '"%s", "%s"\n' "$blobname" "$mt" >> "$LOG_TYPES"
      fi
      par2_create_for_blob "$blobpath" "$blobname"
    fi
  fi

  local reldir; reldir="$(dirname -- "$abs")"
  printf 'mkdir -p "${CARB_RECOVER_TO_DIR}%s" && par2_verify_or_repair "%s" "${CARB_RECOVER_TO_DIR}%s" "%s" "%s" "%s"\n' \
    "$reldir" "$blobpath" "$abs" "$DIR_PAR2" "$blobname" "$CARB_PAR2_CMD" >> "$LOG_RECOV"
}
export -f ingest_one abort stat_filesize hash_stream_sha256

# ---- Build and run 'find' ----------------------------------------------------
_trim_ws() { local s="$1"; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }
build_find_cmd() {
  local root="$1"; local -a cmd=(find "$root")
  local -a prune_paths=()
  [[ "$DIR_BLOBS"     == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_BLOBS")
  [[ "$DIR_META_ROOT" == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_META_ROOT")
  [[ "$DIR_PAR2"      == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_PAR2")
  [[ "$DIR_TMP"       == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_TMP")
  if (( ${#prune_paths[@]} )); then
    cmd+=("("); local first=1; local p; for p in "${prune_paths[@]}"; do
      (( first )) && first=0 || cmd+=(-o); cmd+=(-path "$p"); done; cmd+=(")" -prune -o)
  fi
  if [[ -n "$CARB_EXCLUDE_GLOBS" ]]; then
    IFS=',' read -r -a globs <<<"$CARB_EXCLUDE_GLOBS"
    cmd+=("("); local first=1; local g; for g in "${globs[@]}"; do
      g="$(_trim_ws "$g")"; [[ -z "$g" ]] && continue; (( first )) && first=0 || cmd+=(-o); cmd+=(-name "$g"); done; cmd+=(")" -prune -o)
  fi
  if [[ "$MODE" == "incremental" ]]; then cmd+=(-type f -newer "$TMP_REF" -print0); else cmd+=(-type f -print0); fi
  printf '%s\0' "${cmd[@]}"
}

CMD_ARR=()
while IFS= read -r -d '' part; do CMD_ARR+=("$part"); done < <(build_find_cmd "$CARB_STARTDIR")
# Ensure strict flags in worker subshells:
"${CMD_ARR[@]}" | xargs -0 -I{} -n1 -P "${CARB_JOBS}" bash -Eeuo pipefail -c 'ingest_one "$@"' _ {}

# ---- Aggregate logs and index ------------------------------------------------
LOGDIR="${DIR_META_RUN}/logs"
find "$LOGDIR" -type f -name '*_processed.txt' -exec cat {} + >> "${DIR_META_RUN}/file_processed.txt" 2>/dev/null || true
find "$LOGDIR" -type f -name '*_skipped.txt'   -exec cat {} + >> "${DIR_META_RUN}/file_skipped.txt"   2>/dev/null || true
find "$LOGDIR" -type f -name '*_ingested.txt'  -exec cat {} + >> "${DIR_META_RUN}/file_ingested.txt"  2>/dev/null || true
find "$LOGDIR" -type f -name '*_stat1.txt'     -exec cat {} + >> "${DIR_META_RUN}/file_stat1.txt"     2>/dev/null || true
find "$LOGDIR" -type f -name '*_stat2.txt'     -exec cat {} + >> "${DIR_META_RUN}/file_stat2.txt"     2>/dev/null || true
find "$LOGDIR" -type f -name '*_types.csv'     -exec cat {} + >> "${DIR_META_RUN}/file_types2.csv"    2>/dev/null || true
find "$LOGDIR" -type f -name '*_recover.sh'    -exec cat {} + >> "$RECOVER_SH"                        2>/dev/null || true

awk -F: '{print $1}' "${DIR_META_RUN}/file_ingested.txt" | sort -u >> "${DIR_META_RUN}/INDEX_NEW.txt" || true
cat "${DIR_META_RUN}/INDEX_NEW.txt" >> "${DIR_BLOBS}/INDEX.txt" 2>/dev/null || true

echo "Run metadata:"; cat "${DIR_META_RUN}/carb_startfolder" || true
count_file_lines() { [[ -f "$1" ]] && wc -l < "$1" || echo 0; }
echo "$(count_file_lines "${DIR_META_RUN}/file_processed.txt") files in file_processed.txt"
echo "$(count_file_lines "${DIR_META_RUN}/file_skipped.txt")   files in file_skipped.txt"
echo "$(count_file_lines "${DIR_META_RUN}/file_ingested.txt")  files in file_ingested.txt"

[[ -n "$TMP_REF" ]] && rm -f -- "$TMP_REF" 2>/dev/null || true

echo "Done. Meta: ${DIR_META_RUN}"
echo "Recovery script: ${RECOVER_SH}"
echo ""