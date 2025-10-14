#!/usr/bin/env bash
#
# carb — Content-addressable robust backup + per-file ingester with PAR2
# Usage: ./carb <START_DIR> <REFERENCE_FILE>
#
# Scans START_DIR for files strictly newer than REFERENCE_FILE (by mtime),
# stores bytes in a content-addressed blob store named SIZE_SHA256.data,
# writes rich per-run metadata, and emits a recovery script that verifies
# and (if possible) repairs files using PAR2 before restoring.
#
# Environment knobs (sane, robust defaults):
#   CARB_JOBS             Parallel ingest workers (default: auto-detect cores)
#   CARB_COMMENT          Free-form comment stored in run metadata (optional)
#   CARB_TMPDIR           Temp dir root (default: script_dir/blobs_tmp)
#   CARB_PAR2             1 to enable PAR2 parity (default: 1)
#   CARB_PAR2_REDUNDANCY  PAR2 redundancy percent (default: 10)
#   CARB_PAR2_BLOCKSIZE   PAR2 block size (e.g. 1M); empty = auto
#   CARB_PAR2_CMD         par2 executable (par2 or par2create; default: par2)
#   CARB_ENABLE_MIME      1 to detect MIME types via `file` (default: 1)
#   CARB_EXCLUDE_GLOBS    Comma-separated globs to prune (e.g. "*.tmp,*.swp")
#
# Recovery:
#   export CARB_RECOVER_TO_DIR="/restore/root"
#   /path/to/blobs_meta/v05_<timestamp>/ez_recover_from_here.sh
#
set -Eeuo pipefail
IFS=$'\n\t'

abort() { echo "ERROR: $*" >&2; exit 1; }
trap 'abort "line $LINENO exited with status $?"' ERR

# -------------------------------
# Args & validation
# -------------------------------
if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <START_DIR> <REFERENCE_FILE>" >&2
  exit 64
fi
START_ARG="$1"
REF_FILE="$2"

# Normalize START_DIR to absolute (accepts dir or dir/)
startdir_from_arg() {
  local p="$1"
  local d
  d=$(dirname -- "${p%/}/__SENTRY__")
  [[ "$d" != /* ]] && d="$(pwd)/$d"
  printf '%s\n' "$d"
}
CARB_STARTDIR="$(startdir_from_arg "$START_ARG")"

[[ -d "$CARB_STARTDIR" ]] || abort "Start directory does not exist: $CARB_STARTDIR"
[[ -f "$REF_FILE"       ]] || abort "Reference file does not exist: $REF_FILE"

# -------------------------------
# Paths & run-scoped variables
# -------------------------------
TODAY=$(date "+%Y-%m-%d") || true
STARTTIME=$(date "+%Y-%m-%d_%H_%M_%S") || true
MACHINE=$(uname -n || true)
CARB_COMMENT="${CARB_COMMENT:-}"

# Resolve script base path through symlinks
_resolve() { command -v readlink >/dev/null 2>&1 && readlink -f -- "$1" || python3 - "$1" <<'PY' || echo "$1"
import os,sys
p=sys.argv[1]
print(os.path.abspath(os.path.realpath(p)))
PY
}
SCRIPT_BASE="$(dirname "$(_resolve "${BASH_SOURCE[0]}")")"

DIR_BLOBS="${SCRIPT_BASE}/blobs_sha256"
DIR_TMP="${CARB_TMPDIR:-${SCRIPT_BASE}/blobs_tmp}"
DIR_META_ROOT="${SCRIPT_BASE}/blobs_meta"
DIR_META_RUN="${DIR_META_ROOT}/v05_${STARTTIME}"
DIR_PAR2="${SCRIPT_BASE}/blobs_par2"

PWD_AT_START=$(pwd)

# Jobs (auto-detect cores; fallback 1)
detect_cpus() {
  if command -v nproc >/dev/null 2>&1; then nproc
  elif [[ "$(uname -s)" == "Darwin" ]]; then sysctl -n hw.ncpu
  else echo 1
  fi
}
CARB_JOBS="${CARB_JOBS:-$(detect_cpus)}"
[[ "$CARB_JOBS" =~ ^[1-9][0-9]*$ ]] || CARB_JOBS=1

# PAR2 config
CARB_PAR2="${CARB_PAR2:-1}"
CARB_PAR2_REDUNDANCY="${CARB_PAR2_REDUNDANCY:-10}"
CARB_PAR2_BLOCKSIZE="${CARB_PAR2_BLOCKSIZE:-}"
CARB_PAR2_CMD="${CARB_PAR2_CMD:-par2}"
CARB_ENABLE_MIME="${CARB_ENABLE_MIME:-1}"
CARB_EXCLUDE_GLOBS="${CARB_EXCLUDE_GLOBS:-}"

# Create dirs
mkdir -p -- "$DIR_BLOBS" "$DIR_TMP" "$DIR_META_ROOT" "$DIR_META_RUN" "$DIR_PAR2" "$DIR_META_RUN/logs"

# Initialize run meta
: > "${DIR_META_RUN}/file_processed.txt"   # merged later
: > "${DIR_META_RUN}/file_skipped.txt"
: > "${DIR_META_RUN}/file_ingested.txt"
: > "${DIR_BLOBS}/INDEX.txt"               # merged later (we append per-run at end)

RECOVER_SH="${DIR_META_RUN}/ez_recover_from_here.sh"
printf '#!/usr/bin/env bash\nset -Eeuo pipefail\n: "${CARB_RECOVER_TO_DIR:?set CARB_RECOVER_TO_DIR to a target directory}"\n' > "$RECOVER_SH"
chmod +x "$RECOVER_SH"

printf '%s\n' "$STARTTIME"                          >> "${DIR_META_RUN}/carb_starttime"
printf 'pwd=%s CARB_STARTDIR=%s\n' "$PWD_AT_START" "$CARB_STARTDIR" > "${DIR_META_RUN}/carb_startfolder"
printf 'par2=%s r=%s s=%s cmd=%s jobs=%s\n' \
  "$CARB_PAR2" "$CARB_PAR2_REDUNDANCY" "${CARB_PAR2_BLOCKSIZE:-auto}" "$CARB_PAR2_CMD" "$CARB_JOBS" \
  > "${DIR_META_RUN}/carb_par2_settings"
printf '%s :%s:%s: %s : mnewer ' "$STARTTIME" "$PWD_AT_START" "$CARB_STARTDIR" "$CARB_COMMENT" >> "${DIR_META_ROOT}/ingestedFolders.txt"

# -------------------------------
# Portable helpers (BSD/GNU)
# -------------------------------
stat_epoch_mtime() {
  if stat -c %Y -- "$1" >/dev/null 2>&1; then stat -c %Y -- "$1"; else stat -f %m -- "$1"; fi
}
stat_filesize() {
  if stat -c %s -- "$1" >/dev/null 2>&1; then stat -c %s -- "$1"; else stat -f %z -- "$1"; fi
}
date_from_epoch() {
  local e="$1"
  date -d @"$e" "+%Y-%m-%d_%H_%M_%S" 2>/dev/null || date -r "$e" "+%Y-%m-%d_%H_%M_%S"
}

# Hashing via openssl or shasum
hash_stream_sha256() {
  # reads stdin, prints hex hash to stdout
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 2>/dev/null | awk '{print $NF}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    echo "FATAL: need openssl or shasum for SHA-256" >&2
    return 127
  fi
}

# Cutoff / reference time (strictly newer)
REF_EPOCH="$(stat_epoch_mtime "$REF_FILE")"
REF_STR="$(date_from_epoch "$REF_EPOCH")"
echo "NEWER $REF_EPOCH $REF_STR"
# append formatted date to ingestedFolders line
sed -i.bak "s/mnewer.*/mnewer ${REF_STR}/" "${DIR_META_ROOT}/ingestedFolders.txt" 2>/dev/null || true

# Create a temp ref file with the same mtime, for portable find -newer
TMP_REF="$(mktemp "${DIR_TMP}/ref_${STARTTIME}.XXXX")"
touch -r "$REF_FILE" "$TMP_REF"

# -------------------------------
# Recovery helpers (PAR2-aware)
# -------------------------------
{
cat <<'REC'
par2_verify_or_repair() {
  # $1=src blob path; $2=dest absolute path; $3=par2 dir; $4=blobname; $5=par2 cmd
  local src="$1" dest="$2" pardir="$3" blobname="$4" cmd="$5"
  local base="${pardir}/${blobname}"

  # If no parity, just copy
  if ! ls -1 "${base}.par2" "${base}".vol*.par2 >/dev/null 2>&1; then
    cp -- "$src" "$dest"
    return
  fi

  # Verify; repair if needed in a temp dir
  if command -v "$cmd" >/dev/null 2>&1; then
    if "$cmd" verify -q "${base}.par2" "$src" >/dev/null 2>&1; then
      cp -- "$src" "$dest"
      return
    fi
    local td
    td="$(mktemp -d "${TMPDIR:-/tmp}/carb_repair.XXXX")"
    cp -- "$src" "$td/blob.data"
    if "$cmd" repair -q "${base}.par2" "$td/blob.data" >/dev/null 2>&1; then
      cp -- "$td/blob.data" "$dest"
      rm -rf -- "$td"
      return
    fi
    echo "WARN: PAR2 repair failed for $src; copying original bytes" >&2
    rm -rf -- "$td"
    cp -- "$src" "$dest"
  else
    echo "WARN: par2 not found during recovery; copying without verify" >&2
    cp -- "$src" "$dest"
  fi
}
REC
} >> "$RECOVER_SH"

# -------------------------------
# PAR2 create helper (race-safe)
# -------------------------------
par2_create_for_blob() {
  # $1 = absolute path to blob file, $2 = blobname (e.g., 000..._hash.data)
  [[ "$CARB_PAR2" == "1" ]] || return 0

  local blobpath="$1" blobname="$2" base="${DIR_PAR2}/${blobname}"
  local cmd="$CARB_PAR2_CMD"

  # Find a working command name
  if ! command -v "$cmd" >/dev/null 2>&1; then
    if command -v par2create >/dev/null 2>&1; then cmd=par2create
    elif command -v par2 >/devnull 2>&1; then cmd=par2
    else
      echo "WARN: PAR2 requested but no par2/par2create found" >&2
      return 0
    fi
  fi

  # Skip if parity already exists
  if ls -1 "${base}.par2" "${base}".vol*.par2 >/dev/null 2>&1; then
    return 0
  fi

  # Simple lock dir to avoid duplicate work across workers
  local lock="${DIR_PAR2}/.lock_${blobname}"
  if mkdir "$lock" 2>/dev/null; then
    :
  else
    # another worker is doing it; wait a bit for completion
    local tries=50
    while (( tries-- > 0 )) && [[ ! -e "${base}.par2" ]]; do sleep 0.1; done
    return 0
  fi

  # Build args: -r <percent> [-s <blocksize>] -q
  local args=(-q -r "$CARB_PAR2_REDUNDANCY")
  [[ -n "$CARB_PAR2_BLOCKSIZE" ]] && args+=(-s "$CARB_PAR2_BLOCKSIZE")

  # Create parity set (header + volumes) with stable base name
  if "$cmd" create "${args[@]}" "${base}.par2" "$blobpath"; then
    printf '%s\n' "${blobname}" >> "${DIR_META_RUN}/par2_created.txt"
  else
    echo "WARN: PAR2 creation failed for ${blobname}" >&2
  fi
  rmdir "$lock" 2>/dev/null || true
}

export -f par2_create_for_blob

# -------------------------------
# Per-file ingest function (worker)
# -------------------------------
ingest_one() {
  local src="$1"
  [[ -f "$src" ]] || return 0

  # per-worker logs to avoid write races
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

  # absolute path for clarity
  local abs="$src"
  if [[ "$abs" != /* ]]; then abs="$(cd "$(dirname -- "$src")" && pwd)/$(basename -- "$src")"; fi

  # filesize (zero-padded 18 digits)
  local size
  size="$(stat_filesize "$src")" || return 1
  printf -v size "%018d" "$size"

  # unique temp copy
  local tmpcopy
  tmpcopy="$(mktemp "${DIR_TMP}/${TODAY}.XXXX")"

  # copy + hash in one pass
  local hash
  if ! hash="$(tee -- "$tmpcopy" < "$src" | hash_stream_sha256)"; then
    rm -f -- "$tmpcopy"
    abort "sha256 failed for $src"
  fi

  local blobname="${size}_${hash}.data"
  local blobpath="${DIR_BLOBS}/${blobname}"

  printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_PROC"

  # capture stats (BSD/GNU)
  if stat -s -- "$src" >/dev/null 2>&1; then
    printf '%s %s\n' "$blobname" "$(stat -s -- "$src")" >> "$LOG_STAT1"
    printf '%s %s\n' "$blobname" "$(stat -- "$src")"    >> "$LOG_STAT2"
  else
    printf '%s size=%s mode=%s uid=%s gid=%s mtime=%s\n' \
      "$blobname" "$(stat -c %s -- "$src")" "$(stat -c %a -- "$src")" \
      "$(stat -c %u -- "$src")" "$(stat -c %g -- "$src")" "$(stat -c %Y -- "$src")" \
      >> "$LOG_STAT1"
    stat --printf='%n: %A %h %U %G %s %y\n' -- "$src" \
      | sed "s#^#${blobname} #" >> "$LOG_STAT2"
  fi

  # Install blob atomically with dedup (ln as test-and-set)
  if ln "$tmpcopy" "$blobpath" 2>/dev/null; then
    # ln succeeded: new content (tmpcopy now shares inode with blobpath)
    printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_INGE"
    rm -f -- "$tmpcopy"

    # MIME type (best-effort)
    if [[ "$CARB_ENABLE_MIME" == "1" ]] && command -v file >/dev/null 2>&1; then
      local mt
      mt="$(file -b --mime "$src" 2>/dev/null || file -b --mime-type "$src" 2>/dev/null || true)"
      printf '"%s", "%s"\n' "$blobname" "$mt" >> "$LOG_TYPES"
    fi

    # Create parity (best-effort)
    par2_create_for_blob "$blobpath" "$blobname"
  else
    # blob already exists, discard temp
    printf '%s:%s:%s:%s\n' "$blobname" "$PWD_AT_START" "$CARB_STARTDIR" "$abs" >> "$LOG_SKIP"
    rm -f -- "$tmpcopy"
  fi

  # add a recovery line (PAR2-aware copy)
  local reldir
  reldir="$(dirname -- "$abs")"
  printf 'mkdir -p "${CARB_RECOVER_TO_DIR}%s" && par2_verify_or_repair "%s" "${CARB_RECOVER_TO_DIR}%s" "%s" "%s" "%s"\n' \
    "$reldir" "$blobpath" "$abs" "$DIR_PAR2" "$blobname" "$CARB_PAR2_CMD" >> "$LOG_RECOV"
}

export -f ingest_one abort stat_filesize hash_stream_sha256
export TODAY DIR_TMP DIR_BLOBS DIR_META_RUN DIR_PAR2 PWD_AT_START CARB_STARTDIR CARB_PAR2_CMD

# -------------------------------
# Build find command (excludes)
# -------------------------------
build_find_cmd() {
  local root="$1"
  shift || true
  local -a cmd=(find "$root")

  # prune our own internal dirs if they sit under STARTDIR
  local -a prune_paths=()
  [[ "$DIR_BLOBS"     == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_BLOBS")
  [[ "$DIR_META_ROOT" == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_META_ROOT")
  [[ "$DIR_PAR2"      == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_PAR2")
  [[ "$DIR_TMP"       == "$CARB_STARTDIR"* ]] && prune_paths+=("$DIR_TMP")

  if (( ${#prune_paths[@]} )); then
    cmd+=("(")
    local first=1
    for p in "${prune_paths[@]}"; do
      if (( first )); then first=0; else cmd+=(-o); fi
      cmd+=(-path "$p")
    done
    cmd+=(")" -prune -o)
  fi

  # user-specified globs to prune
  if [[ -n "$CARB_EXCLUDE_GLOBS" ]]; then
    IFS=',' read -r -a globs <<<"$CARB_EXCLUDE_GLOBS"
    cmd+=("(")
    local first=1
    for g in "${globs[@]}"; do
      g="${g## }"; g="${g%% }"
      [[ -z "$g" ]] && continue
      if (( first )); then first=0; else cmd+=(-o); fi
      cmd+=(-name "$g")
    done
    cmd+=(")" -prune -o)
  fi

  # main predicate: files newer than TMP_REF
  cmd+=(-type f -newer "$TMP_REF" -print0)
  printf '%s\0' "${cmd[@]}"
}

# -------------------------------
# Walk newer files and ingest (parallel)
# -------------------------------
# shellcheck disable=SC2046
eval "set -- $(build_find_cmd "$CARB_STARTDIR" | tr '\0' ' ')"
# run the constructed find command, pipe to xargs parallel workers
"${@}" \
| xargs -0 -I{} -n1 -P "${CARB_JOBS}" bash -c 'ingest_one "$@"' _ {}

# -------------------------------
# Merge per-worker logs atomically
# -------------------------------
LOGDIR="${DIR_META_RUN}/logs"
# Merge with simple concatenation (no ordering requirement)
for f in processed skipped ingested; do
  cat "${LOGDIR}"/_*"${f}".txt 2>/dev/null >> "${DIR_META_RUN}/file_${f}.txt" || true
done
cat "${LOGDIR}"/_*stat1.txt 2>/dev/null >> "${DIR_META_RUN}/file_stat1.txt" || true
cat "${LOGDIR}"/_*stat2.txt 2>/dev/null >> "${DIR_META_RUN}/file_stat2.txt" || true
cat "${LOGDIR}"/_*types.csv 2>/dev/null >> "${DIR_META_RUN}/file_types2.csv" || true
# Append recovery lines
cat "${LOGDIR}"/_*recover.sh 2>/dev/null >> "$RECOVER_SH" || true

# Build a per-run index of NEW blobs only (from ingested)
# and append unique blobnames to global INDEX.txt (de-dup within this run)
awk -F: '{print $1}' "${DIR_META_RUN}/file_ingested.txt" \
  | sort -u >> "${DIR_META_RUN}/INDEX_NEW.txt" || true
# Append to global index (no need for sort; duplicates harmless, but keep tidy)
cat "${DIR_META_RUN}/INDEX_NEW.txt" >> "${DIR_BLOBS}/INDEX.txt" 2>/dev/null || true

# -------------------------------
# Summaries
# -------------------------------
echo "Run metadata:"
cat "${DIR_META_RUN}/carb_startfolder" || true

count_file_lines() { [[ -f "$1" ]] && wc -l < "$1" || echo 0; }
echo "$(count_file_lines "${DIR_META_RUN}/file_processed.txt") files in file_processed.txt"
echo "$(count_file_lines "${DIR_META_RUN}/file_skipped.txt")   files in file_skipped.txt"
echo "$(count_file_lines "${DIR_META_RUN}/file_ingested.txt")  files in file_ingested.txt"

# Cleanup
rm -f -- "$TMP_REF" 2>/dev/null || true

echo "Done. Meta: ${DIR_META_RUN}"
echo "Recovery script: ${RECOVER_SH}"

# Append a final newline to recovery for niceness
echo "" >> "$RECOVER_SH"