#!/usr/bin/env bash
#------------------------------------------------------------------------------
# carb — Content-addressable robust backup + per-file ingester with PAR2
#
# OVERVIEW
#   Scans a start directory, ingests every regular file, and stores its bytes
#   once under a content-addressable blob name "<padded_size>_<sha256>.data".
#   Subsequent duplicates hardlink to the same blob. A per-run metadata folder
#   records provenance, stats, MIME types (optional), and generates a recovery
#   script that can verify/repair bytes with PAR2 and restore original paths.
#
#   Modes:
#     • Full         — ingest all files found under START_DIR
#     • Incremental  — ingest only files newer than a reference file’s mtime
#
# USAGE
#     Incremental:  carb <START_DIR> <REFERENCE_FILE>
#     Full backup:  carb <START_DIR>
#                   carb <START_DIR> --full
#
# OUTPUT LAYOUT (relative to this script’s directory)
#     blobs_sha256/                 # content-addressed data blobs
#       INDEX.txt                   # appended list of blob names seen across runs
#     blobs_par2/                   # .par2 and .vol*.par2 parity sets per blob
#     blobs_tmp/                    # temporary workspace
#     blobs_meta/
#       v05_<YYYY-MM-DD_HH_MM_SS>/  # per-run metadata + logs
#         file_processed.txt        # blobname:cwd:startdir:absolute_path (all seen)
#         file_skipped.txt          # lines for files deduped by existing blob
#         file_ingested.txt         # lines for files that produced/linked blob
#         file_stat1.txt            # portable stat summary (key=value-ish)
#         file_stat2.txt            # human stat summary (platform-native)
#         file_types2.csv           # "blobname","mime-type" (if enabled)
#         par2_created.txt          # blobnames for which PAR2 was emitted
#         recover.sh                # self-contained recovery helper
#         carb_starttime            # run timestamp
#         carb_startfolder          # original PWD and normalized start dir
#         carb_settings             # run settings (mode, par2 params, jobs)
#     blobs_meta/ingestedFolders.txt # append-only log of ingested roots
#
# DEPENDENCIES
#   Required:  find, xargs, awk, sed, tee, mktemp, ln, cp, stat, date,
#              openssl (or shasum)
#   Optional:  file (for MIME detection)
#   If PAR2 on: par2cmdline (par2create/par2)
#
# AUTO-INSTALL (best-effort; can be disabled)
#   Attempts to install missing tools via detected package manager (apt, dnf,
#   yum, pacman, zypper, apk, brew, port) if allowed by the flags below.
#
# ENVIRONMENT
#   CARB_JOBS               # worker parallelism (default: CPU cores)
#   CARB_PAR2=0|1           # enable parity creation (default: 1)
#   CARB_PAR2_REDUNDANCY    # % parity if fixed; else adaptive (default: 10)
#   CARB_PAR2_BLOCKSIZE     # bytes or "auto"/"" for adaptive (default: auto)
#   CARB_PAR2_CMD           # par2 binary name (default: par2)
#   CARB_ENABLE_MIME=0|1    # detect MIME using 'file' (default: 1)
#   CARB_EXCLUDE_GLOBS      # comma-separated globs to prune during find
#   CARB_TMPDIR             # override tmp directory path
#   CARB_AUTOINSTALL_ASK    # 1 to prompt (default), 0 to skip prompting
#   CARB_AUTOINSTALL_YES    # non-empty or "y" to auto-yes install
#   CARB_PKG_MANAGER        # force a package manager selection
#   CARB_COMMENT            # freeform note stored with run metadata
#
# RECOVERY
#   Per-run recover.sh requires: CARB_RECOVER_TO_DIR=<destination>
#   It will verify/repair bytes with PAR2 if available and recreate the original
#   directory layout under CARB_RECOVER_TO_DIR.
#
# EXIT CODES
#   0   success
#   64  usage error
#   69  missing required tools after attempted install
#   other nonzero codes may come from underlying commands; trapped to show line
#
# EXAMPLES
#   Full:        CARB_COMMENT="NAS mirror" ./carb /data
#   Incremental: ./carb /data /var/backups/last-full.marker
#   Exclude:     CARB_EXCLUDE_GLOBS="*.tmp,.git,node_modules" ./carb ~/projects --full
#
# NOTES
#   • Content identity is (size, sha256) to avoid accidental hash collisions on
#     truncated pipes; size is left-padded to 18 digits in blob names.
#   • PAR2 block size and parity % are adaptive unless explicitly set.
#   • The script prunes its own output directories when scanning inside START_DIR.
#------------------------------------------------------------------------------

if [[ "${1-}" == "--help" ]]; then
  cat <<'EOF'
carb — Content-addressable robust backup and per-file ingester with PAR2

Usage:
  carb <START_DIR> [REFERENCE_FILE | --full]

Modes:
  Incremental  : backup only files newer than REFERENCE_FILE
  Full backup  : backup all files under START_DIR

Environment variables:
  CARB_PAR2=0|1             Enable/disable PAR2 creation (default 1)
  CARB_JOBS=<n>             Number of parallel workers (default: CPU cores)
  CARB_EXCLUDE_GLOBS=<g>    Comma-separated exclusion globs
  CARB_ENABLE_MIME=0|1      MIME detection using 'file' (default 1)
  CARB_COMMENT=<text>       Optional run comment

Examples:
  carb /data
  carb /data /var/backups/last.marker
  CARB_EXCLUDE_GLOBS=".git,node_modules" carb ~/projects --full

EOF
  exit 0
fi

if [[ "${1-}" == "--version" ]]; then
  echo "carb 1.0 (2025-10-29)"
  exit 0
fi

set -Eeuo pipefail
IFS=$'\n\t'

abort() { echo "ERROR: $*" >&2; exit 1; }
trap 'abort "line $LINENO exited with status $?"' ERR

have() { command -v "$1" >/dev/null 2>&1; }
is_tty() { [[ -t 0 && -t 1 ]]; }

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage:" >&2
  echo "  Incremental:  $0 <START_DIR> <REFERENCE_FILE>" >&2
  echo "  Full backup:  $0 <START_DIR>  (or: $0 <START_DIR> --full)" >&2
  exit 64
fi

START_ARG="$1"
REF_OR_FLAG="${2-}"

startdir_from_arg() {
  local p="$1"
  local d
  d=$(dirname -- "${p%/}/__SENTRY__")
  [[ "$d" != /* ]] && d="$(pwd)/$d"
  printf '%s\n' "$d"
}
CARB_STARTDIR="$(startdir_from_arg "$START_ARG")"
[[ -d "$CARB_STARTDIR" ]] || abort "Start directory does not exist: $CARB_STARTDIR"

MODE="incremental"
if [[ $# -eq 1 || "$REF_OR_FLAG" == "--full" ]]; then
  MODE="full"
else
  REF_FILE="$REF_OR_FLAG"
  [[ -f "$REF_FILE" ]] || abort "Reference file does not exist: $REF_FILE"
fi

TODAY=$(date "+%Y-%m-%d") || true
STARTTIME=$(date "+%Y-%m-%d_%H_%M_%S") || true
CARB_COMMENT="${CARB_COMMENT:-}"

_resolve() { command -v readlink >/dev/null 2>&1 && readlink -f -- "$1" || python3 - "$1" <<'PY' || echo "$1"
import os,sys
p=sys.argv[1]
print(os.path.abspath(os.path.realpath(p)))
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
  if command -v nproc >/dev/null 2>&1; then nproc
  elif [[ "$(uname -s)" == "Darwin" ]]; then sysctl -n hw.ncpu
  else echo 1
  fi
}
CARB_JOBS="${CARB_JOBS:-$(detect_cpus)}"
[[ "$CARB_JOBS" =~ ^[1-9][0-9]*$ ]] || CARB_JOBS=1

CARB_PAR2="${CARB_PAR2:-1}"
CARB_PAR2_REDUNDANCY="${CARB_PAR2_REDUNDANCY:-10}"
CARB_PAR2_BLOCKSIZE="${CARB_PAR2_BLOCKSIZE:-}"
CARB_PAR2_CMD="${CARB_PAR2_CMD:-par2}"
CARB_ENABLE_MIME="${CARB_ENABLE_MIME:-1}"
CARB_EXCLUDE_GLOBS="${CARB_EXCLUDE_GLOBS:-}"
CARB_AUTOINSTALL_ASK="${CARB_AUTOINSTALL_ASK:-1}"
CARB_AUTOINSTALL_YES="${CARB_AUTOINSTALL_YES:-}"
CARB_PKG_MANAGER="${CARB_PKG_MANAGER:-}"

dep_check_and_maybe_install() {
  local -a missing_req=()
  local -a missing_opt=()
  local -a req=(find xargs awk sed tee mktemp ln cp stat date)
  local c
  for c in "${req[@]}"; do have "$c" || missing_req+=("$c"); done
  if ! have openssl && ! have shasum; then missing_req+=("openssl (or shasum)"); fi
  if [[ "$CARB_ENABLE_MIME" == "1" ]] && ! have file; then missing_opt+=("file"); fi
  if [[ "$CARB_PAR2" == "1" ]]; then
    if ! have "$CARB_PAR2_CMD" && ! have par2create && ! have par2; then missing_req+=("par2cmdline"); fi
  fi
  if (( ${#missing_req[@]} == 0 && ${#missing_opt[@]} == 0 )); then return 0; fi
  echo "==> Pre-flight dependency check" >&2
  (( ${#missing_req[@]} )) && { echo "Missing REQUIRED tools:" >&2; printf '  - %s\n' "${missing_req[@]}" >&2; }
  (( ${#missing_opt[@]} )) && { echo "Missing OPTIONAL tools:" >&2; printf '  - %s\n' "${missing_opt[@]}" >&2; }

  local mgr="${CARB_PKG_MANAGER}"
  if [[ -z "$mgr" ]]; then
    if have apt-get; then mgr="apt"
    elif have dnf; then mgr="dnf"
    elif have yum; then mgr="yum"
    elif have pacman; then mgr="pacman"
    elif have zypper; then mgr="zypper"
    elif have apk; then mgr="apk"
    elif have brew; then mgr="brew"
    elif have port; then mgr="port"
    else mgr=""
    fi
  fi

  local -a install_pkgs=()
  if ! have openssl && ! have shasum; then case "$mgr" in apt|dnf|yum|zypper|apk|pacman|brew|port) install_pkgs+=("openssl");; esac; fi
  if [[ "$CARB_ENABLE_MIME" == "1" ]] && ! have file; then case "$mgr" in apt|dnf|yum|zypper|apk|pacman|port) install_pkgs+=("file");; brew) install_pkgs+=("file-formula");; esac; fi
  if [[ "$CARB_PAR2" == "1" ]] && ! have "$CARB_PAR2_CMD" && ! have par2create && ! have par2; then case "$mgr" in apt|zypper|apk|brew|port) install_pkgs+=("par2");; dnf|yum|pacman) install_pkgs+=("par2cmdline");; esac; fi

  if (( ${#install_pkgs[@]} )); then
    local do_install=""
    if [[ "${CARB_AUTOINSTALL_ASK}" == "0" ]]; then
      do_install="no"
    else
      do_install="${CARB_AUTOINSTALL_YES:-}"
      if [[ -z "$do_install" ]] && is_tty; then
        echo -n "Attempt to install missing packages (${install_pkgs[*]}) via ${mgr:-<unknown>}? [y/N] " >&2
        read -r do_install || true
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
      if [[ -n "$cmd" ]]; then bash -c "$cmd" || abort "Automatic installation failed. Please install packages manually: ${install_pkgs[*]}"; else echo "No supported package manager detected for auto-install." >&2; fi
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
: > "${DIR_BLOBS}/INDEX.txt"

RECOVER_SH="${DIR_META_RUN}/recover.sh"
printf '#!/usr/bin/env bash\nset -Eeuo pipefail\n: "${CARB_RECOVER_TO_DIR:?set CARB_RECOVER_TO_DIR to a target directory}"\n' > "$RECOVER_SH"
chmod +x "$RECOVER_SH"

printf '%s\n' "$STARTTIME"                          >> "${DIR_META_RUN}/carb_starttime"
printf 'pwd=%s CARB_STARTDIR=%s\n' "$PWD_AT_START" "$CARB_STARTDIR" > "${DIR_META_RUN}/carb_startfolder"
printf 'mode=%s par2=%s r=%s s=%s cmd=%s jobs=%s\n' \
  "$MODE" "$CARB_PAR2" "$CARB_PAR2_REDUNDANCY" "${CARB_PAR2_BLOCKSIZE:-auto}" "$CARB_PAR2_CMD" "$CARB_JOBS" \
  > "${DIR_META_RUN}/carb_settings"
printf '%s :%s:%s: %s : %s\n' "$STARTTIME" "$PWD_AT_START" "$CARB_STARTDIR" "$CARB_COMMENT" "$MODE" >> "${DIR_META_ROOT}/ingestedFolders.txt"

stat_epoch_mtime() { if stat -c %Y -- "$1" >/dev/null 2>&1; then stat -c %Y -- "$1"; else stat -f %m -- "$1"; fi; }
stat_filesize() { if stat -c %s -- "$1" >/dev/null 2>&1; then stat -c %s -- "$1"; else stat -f %z -- "$1"; fi; }
date_from_epoch() { local e="$1"; date -d @"$e" "+%Y-%m-%d_%H_%M_%S" 2>/dev/null || date -r "$e" "+%Y-%m-%d_%H_%M_%S"; }

hash_stream_sha256() {
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 - 2>/dev/null | awk '{print $NF}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    echo "FATAL: need openssl or shasum for SHA-256" >&2
    return 127
  fi
}

TMP_REF=""
if [[ "$MODE" == "incremental" ]]; then
  REF_EPOCH="$(stat_epoch_mtime "$REF_FILE")"
  REF_STR="$(date_from_epoch "$REF_EPOCH")"
  echo "NEWER $REF_EPOCH $REF_STR"
  sed -i.bak "s/ $MODE .*/ $MODE ref=${REF_STR}/" "${DIR_META_ROOT}/ingestedFolders.txt" 2>/dev/null || true
  TMP_REF="$(mktemp "${DIR_TMP}/ref_${STARTTIME}.XXXX")"
  touch -r "$REF_FILE" "$TMP_REF"
else
  echo "FULL backup mode (no reference cutoff)"
  sed -i.bak "s/ $MODE .*/ $MODE full/" "${DIR_META_ROOT}/ingestedFolders.txt" 2>/dev/null || true
fi

{
cat <<'REC'
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
  local cmd="$prefer"
  [[ -n "$cmd" ]] || cmd="$(_select_par2_cmd)"

  if ! ls -1 "${base}.par2" "${base}".vol*.par2 >/dev/null 2>&1; then
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

_next_pow2() {
  local n="$1"; (( n < 1 )) && { echo 1; return; }
  local p=1; while (( p < n )); do (( p <<= 1 )); done; echo "$p"
}

_par2_plan_for_size() {
  # Force decimal even if the size string has leading zeros (e.g. 000...018)
  local input="${1:-0}"
  local sz_dec
  sz_dec=$((10#${input}))

  # Constants / defaults
  local TARGET_DATA_SLICES=16
  local MIN_PARITY_SLICES=4
  local MIN_BLOCK=512
  local MAX_BLOCK=$((4*1024*1024))
  local DEFAULT_R="${CARB_PAR2_REDUNDANCY:-10}"

  # If user fixed both blocksize and redundancy, just return them
  if [[ -n "${CARB_PAR2_BLOCKSIZE:-}" && "${CARB_PAR2_BLOCKSIZE}" != "auto" && -n "${CARB_PAR2_REDUNDANCY:-}" ]]; then
    echo "${CARB_PAR2_BLOCKSIZE} ${CARB_PAR2_REDUNDANCY}"
    return
  fi

  # If user fixed only the blocksize, compute a safe redundancy
  if [[ -n "${CARB_PAR2_BLOCKSIZE:-}" && "${CARB_PAR2_BLOCKSIZE}" != "auto" ]]; then
    local bs="${CARB_PAR2_BLOCKSIZE}"
    local ds=$(( (sz_dec + bs - 1) / bs )); (( ds < 1 )) && ds=1
    local r="${DEFAULT_R}"
    local ps=$(( (ds * r + 99) / 100 ))
    if (( ps < MIN_PARITY_SLICES )); then
      r=$(( (MIN_PARITY_SLICES * 100 + ds - 1) / ds ))
      (( r > 80 )) && r=80
    fi
    echo "${bs} ${r}"
    return
  fi

  # Fully automatic (adaptive) blocksize + redundancy
  local bs=$(( sz_dec / TARGET_DATA_SLICES ))
  (( bs < MIN_BLOCK )) && bs="${MIN_BLOCK}"
  
  # Round block size up to next power of two
  _next_pow2() { local n="$1"; (( n < 1 )) && { echo 1; return; }; local p=1; while (( p < n )); do (( p <<= 1 )); done; echo "$p"; }
  bs="$(_next_pow2 "$bs")"

  (( bs > MAX_BLOCK )) && bs="${MAX_BLOCK}"

  local ds=$(( (sz_dec + bs - 1) / bs )); (( ds < 1 )) && ds=1
  local r="${DEFAULT_R}"
  local ps=$(( (ds * r + 99) / 100 ))
  if (( ps < MIN_PARITY_SLICES )); then
    r=$(( (MIN_PARITY_SLICES * 100 + ds - 1) / ds ))
    (( r > 80 )) && r=80
  fi

  echo "${bs} ${r}"
}
export -f _next_pow2 _par2_plan_for_size

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
  if ls -1 "${base}.par2" "${base}".vol*.par2 >/dev/null 2>&1; then return 0; fi

  local lock="${DIR_PAR2}/.lock_${blobname}"
  if ! mkdir "$lock" 2>/dev/null; then
    local tries=50; while (( tries-- > 0 )) && [[ ! -e "${base}.par2" ]]; do sleep 0.1; done
    return 0
  fi

  local bs_opt="" r_opt=""
  local s_bytes="" r_pct=""
  if [[ -z "${CARB_PAR2_BLOCKSIZE:-}" || "${CARB_PAR2_BLOCKSIZE:-}" == "auto" ]]; then
    local fsz="${blobname%%_*}"; fsz="${fsz#0}"; [[ -z "$fsz" ]] && fsz=0
    read -r s_bytes r_pct < <(_par2_plan_for_size "$fsz")
    bs_opt="-s${s_bytes}"
    r_opt="-r${r_pct}"
  else
    [[ -n "${CARB_PAR2_BLOCKSIZE:-}"  ]] && bs_opt="-s${CARB_PAR2_BLOCKSIZE}"
    local r_use="${CARB_PAR2_REDUNDANCY:-10}"
    r_opt="-r${r_use}"
  fi

  local args=(-q -B /)
  [[ -n "$r_opt" ]] && args+=("$r_opt")
  [[ -n "$bs_opt" ]] && args+=("$bs_opt")

  if "$cmd" create "${args[@]}" "${base}.par2" -- "$blobpath" 2>>"${DIR_META_RUN}/par2.stderr"; then
    printf '%s\n' "${blobname}" >> "${DIR_META_RUN}/par2_created.txt"
  else
    echo "WARN: PAR2 creation failed for ${blobname}" >&2
  fi
  rmdir "$lock" 2>/dev/null || true
}
export -f par2_create_for_blob

ingest_one() {
  local src="$1"
  [[ -f "$src" ]] || return 0

  local wid="$$"
  local LOGDIR="${DIR_META_RUN}/logs"
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

  local blobname="${size}_${hash}.data"
  local blobpath="${DIR_BLOBS}/${blobname}"

  printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_PROC"

  if stat -s "$src" >/dev/null 2>&1; then
    printf '%s %s\n' "$blobname" "$(stat -s "$src")" >> "$LOG_STAT1"
    stat "$src" 2>/dev/null | sed "s#^#${blobname} #" >> "$LOG_STAT2"
  else
    printf '%s size=%s mode=%s uid=%s gid=%s mtime=%s\n' \
      "$blobname" "$(stat -c %s -- "$src")" "$(stat -c %a -- "$src")" \
      "$(stat -c %u -- "$src")" "$(stat -c %g -- "$src")" "$(stat -c %Y -- "$src")" >> "$LOG_STAT1"
    stat --printf='%n: %A %h %U %G %s %y\n' -- "$src" | sed "s#^#${blobname} #" >> "$LOG_STAT2"
  fi

  if ln "$tmpcopy" "$blobpath" 2>/dev/null; then
    printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_INGE"
    rm -f -- "$tmpcopy"
    if [[ "$CARB_ENABLE_MIME" == "1" ]] && command -v file >/dev/null 2>&1; then
      local mt; mt="$(file -b --mime-type "$src" 2>/dev/null || file -b --mime "$src" 2>/dev/null || true)"
      printf '"%s", "%s"\n' "$blobname" "$mt" >> "$LOG_TYPES"
    fi
    par2_create_for_blob "$blobpath" "$blobname"
  else
    if [[ -e "$blobpath" ]]; then
      printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_SKIP"
      rm -f -- "$tmpcopy"
      if [[ "$CARB_PAR2" == "1" ]]; then
        if ! ls -1 "${DIR_PAR2}/${blobname}.par2" "${DIR_PAR2}/${blobname}".vol*.par2 >/dev/null 2>&1; then
          par2_create_for_blob "$blobpath" "$blobname"
        fi
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
      if [[ "$CARB_ENABLE_MIME" == "1" ]] && command -v file >/dev/null 2>&1; then
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
export TODAY DIR_TMP DIR_BLOBS DIR_META_RUN DIR_PAR2 PWD_AT_START CARB_STARTDIR \
       CARB_PAR2_CMD CARB_ENABLE_MIME CARB_PAR2 CARB_PAR2_REDUNDANCY CARB_PAR2_BLOCKSIZE

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
"${CMD_ARR[@]}" | xargs -0 -I{} -n1 -P "${CARB_JOBS}" bash -c 'ingest_one "$@"' _ {}

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