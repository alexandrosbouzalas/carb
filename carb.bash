#!/usr/bin/env bash
#
# ba.sh — Combined backup driver + per-file ingester
# Usage: ./ba.sh <START_DIR> <REFERENCE_FILE>
#
# Backs up all files under START_DIR whose mtime is newer than REFERENCE_FILE
# into a content-addressed blob store, writing per-run metadata and a
# recovery script that can reconstruct original paths.
#
# Environment knobs:
#   BA_JOBS        Parallel file processes for ingest (default: 1; keep at 1 unless you add locking)
#   BA_COMMENT     Free-form comment stored in run metadata (optional)

set -Eeuo pipefail
IFS=$'\n\t'

abort() {
  echo "ERROR: $*" >&2
  exit 1
}
trap 'abort "line $LINENO exited with status $?"' ERR

# -------------------------------
# Args & basic validation
# -------------------------------
if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <START_DIR> <REFERENCE_FILE>" >&2
  exit 64
fi

START_ARG="$1"
REF_FILE="$2"

# Normalize START_DIR like your original (accepts dir or path ending with '/')
# but also canonicalize to an absolute path.
startdir_from_arg() {
  local p="$1"
  # ensure p is a directory path even if user passed a trailing slash
  local d
  d=$(dirname -- "${p%/}/__SENTRY__")
  # turn into absolute
  if [[ "$d" != /* ]]; then
    d="$(pwd)/$d"
  fi
  printf '%s\n' "$d"
}
BA_STARTDIR="$(startdir_from_arg "$START_ARG")"

[[ -d "$BA_STARTDIR" ]] || abort "Start directory does not exist: $BA_STARTDIR"
[[ -f "$REF_FILE"    ]] || abort "Reference file does not exist: $REF_FILE"

# -------------------------------
# Paths & run-scoped variables
# -------------------------------
BA_TODAY=$(date "+%Y-%m-%d")
BA_STARTTIME=$(date "+%Y-%m-%d_%H_%M_%S")
BA_MACHINE=$(uname -n || true)
BA_COMMENT="${BA_COMMENT:-}"
# Resolve script base path even through symlinks when possible
_resolve() { command -v readlink >/dev/null 2>&1 && readlink -f -- "$1" || python3 - "$1" <<'PY' || echo "$1"
import os,sys
p=sys.argv[1]
print(os.path.abspath(os.path.realpath(p)))
PY
}
BA_SCRIPT_BASE_PATH="$(dirname "$(_resolve "${BASH_SOURCE[0]}")")"
BA_DIR_DATA="${BA_SCRIPT_BASE_PATH}/blobs_sha256"
BA_BLOBS_TMPDIR="${BA_SCRIPT_BASE_PATH}/blobs_tmp"
BA_METAROOT="${BA_SCRIPT_BASE_PATH}/blobs_meta"
BA_METADIR="${BA_METAROOT}/v05_${BA_STARTTIME}"
BA_PWD=$(pwd)
BA_JOBS="${BA_JOBS:-1}"

mkdir -p -- "$BA_DIR_DATA" "$BA_BLOBS_TMPDIR" "$BA_METAROOT" "$BA_METADIR"

# Initialize run meta files so later wc -l won’t fail
: > "${BA_METADIR}/file_processed.txt"
: > "${BA_METADIR}/file_skipped.txt"
: > "${BA_METADIR}/file_ingested.txt"
: > "${BA_DIR_DATA}/INDEX.txt"                # ensure exists for append
RECOVER_SH="${BA_METADIR}/ez_recover_from_here.sh"
printf '#!/usr/bin/env bash\nset -Eeuo pipefail\n: "${BA_RECOVER_TO_DIR:?set BA_RECOVER_TO_DIR to a target directory}"\n' > "$RECOVER_SH"
chmod +x "$RECOVER_SH"

printf '%s\n' "$BA_STARTTIME"                          >> "${BA_METADIR}/ba_starttime"
printf 'pwd=%s BA_STARTDIR=%s\n' "$BA_PWD" "$BA_STARTDIR" > "${BA_METADIR}/ba_startfolder"
printf '%s :%s:%s: %s : mnewer ' "$BA_STARTTIME" "$BA_PWD" "$BA_STARTDIR" "$BA_COMMENT" >> "${BA_METAROOT}/ingestedFolders.txt"

# -------------------------------
# Portable stat helpers (BSD/GNU)
# -------------------------------
stat_epoch_mtime() {
  # prints mtime (epoch seconds)
  if stat -c %Y -- "$1" >/dev/null 2>&1; then
    stat -c %Y -- "$1"
  else
    stat -f %m -- "$1"
  fi
}
stat_filesize() {
  # prints size in bytes
  if stat -c %s -- "$1" >/dev/null 2>&1; then
    stat -c %s -- "$1"
  else
    stat -f %z -- "$1"
  fi
}

REF_EPOCH="$(stat_epoch_mtime "$REF_FILE")"
REF_STR="$(date -d @"$REF_EPOCH" "+%Y-%m-%d_%H_%M_%S" 2>/dev/null || date -r "$REF_EPOCH" "+%Y-%m-%d_%H_%M_%S")"
echo "NEWER $REF_EPOCH $REF_STR"
# Append formatted date to the line we already started
sed -i.bak "s/mnewer.*/mnewer ${REF_STR}/" "${BA_METAROOT}/ingestedFolders.txt" 2>/dev/null || true

# Create a temp “reference timestamp” file and set its mtime to REF_FILE’s,
# so we can use portable `find -newer` everywhere.
TMP_REF=$(mktemp "${BA_BLOBS_TMPDIR}/ref_${BA_STARTTIME}.XXXX")
touch -r "$REF_FILE" "$TMP_REF"

# -------------------------------
# Per-file ingest function
# -------------------------------
ingest_one() {
  local src="$1"
  [[ -f "$src" ]] || return 0

  # record processed
  # (Use absolute path for clarity in logs)
  local abs="$src"
  if [[ "$abs" != /* ]]; then abs="$(cd "$(dirname -- "$src")" && pwd)/$(basename -- "$src")"; fi

  # filesize (zero-padded 18 digits)
  local size
  size="$(stat_filesize "$src")" || return 1
  printf -v size "%018d" "$size"

  # unique temp copy
  local tmpcopy
  tmpcopy="$(mktemp "${BA_BLOBS_TMPDIR}/${BA_TODAY}.XXXX")"

  # copy + hash in one pass; robustly parse last field for hash
  # (openssl outputs "...= <hash>" or "<hash> *-")
  local hash
  if ! hash="$(tee -- "$tmpcopy" < "$src" | openssl dgst -sha256 2>/dev/null | awk '{print $NF}')"; then
    rm -f -- "$tmpcopy"
    abort "openssl sha256 failed for $src"
  fi

  local blobname="${size}_${hash}.data"
  local blobpath="${BA_DIR_DATA}/${blobname}"

  printf '%s:%s:%s:%s\n' "$blobname" "$BA_PWD" "$BA_STARTDIR" "$abs" >> "${BA_METADIR}/file_processed.txt"

  # always capture stats (BSD/GNU: make outputs predictable)
  if stat -s -- "$src" >/dev/null 2>&1; then
    printf '%s %s\n' "$blobname" "$(stat -s -- "$src")" >> "${BA_METADIR}/file_stat1.txt"
    printf '%s %s\n' "$blobname" "$(stat -- "$src")"    >> "${BA_METADIR}/file_stat2.txt"
  else
    # GNU fallback approximations
    printf '%s size=%s mode=%s uid=%s gid=%s mtime=%s\n' \
      "$blobname" "$(stat -c %s -- "$src")" "$(stat -c %a -- "$src")" \
      "$(stat -c %u -- "$src")" "$(stat -c %g -- "$src")" "$(stat -c %Y -- "$src")" \
      >> "${BA_METADIR}/file_stat1.txt"
    stat --printf='%n: %A %h %U %G %s %y\n' -- "$src" \
      | sed "s#^#${blobname} #" >> "${BA_METADIR}/file_stat2.txt"
  fi

  if [[ -f "$blobpath" ]]; then
    # already have this content
    printf '%s:%s:%s:%s\n' "$blobname" "$BA_PWD" "$BA_STARTDIR" "$abs" >> "${BA_METADIR}/file_skipped.txt"
    rm -f -- "$tmpcopy"
  else
    # new content
    printf '%s:%s:%s:%s\n' "$blobname" "$BA_PWD" "$BA_STARTDIR" "$abs" >> "${BA_METADIR}/file_ingested.txt"
    mv -- "$tmpcopy" "$blobpath"
    printf '%s\n' "$blobname" >> "${BA_DIR_DATA}/INDEX.txt"

    # MIME type (best-effort)
    if command -v file >/dev/null 2>&1; then
      local mt
      mt="$(file -b --mime "$src" 2>/dev/null || file -b --mime-type "$src" 2>/dev/null || true)"
      printf '"%s", "%s"\n' "$blobname" "$mt" >> "${BA_METADIR}/file_types2.csv"
    fi
  fi

  # add a recovery line
  # It recreates source dir under ${BA_RECOVER_TO_DIR} and copies blob back
  local reldir
  reldir="$(dirname -- "$abs")"
  printf 'mkdir -p "${BA_RECOVER_TO_DIR}%s" && cp "%s" "${BA_RECOVER_TO_DIR}%s"\n' \
    "$reldir" "$blobpath" "$abs" >> "$RECOVER_SH"
}

export -f ingest_one abort stat_filesize
export BA_TODAY BA_BLOBS_TMPDIR BA_DIR_DATA BA_METADIR BA_PWD BA_STARTDIR RECOVER_SH

# -------------------------------
# Walk newer files and ingest
# -------------------------------
# Exclude our own meta/blob dirs if they sit inside STARTDIR (defensive).
# Use -newer with the tmp ref file to be portable.
# Null-delimit to be safe for weird filenames.
if [[ "$BA_DIR_DATA" == "$BA_STARTDIR"* || "$BA_METAROOT" == "$BA_STARTDIR"* ]]; then
  # prune internal dirs
  find "$BA_STARTDIR" \( -path "$BA_DIR_DATA" -o -path "$BA_METAROOT" \) -prune -o -type f -newer "$TMP_REF" -print0
else
  find "$BA_STARTDIR" -type f -newer "$TMP_REF" -print0
fi \
| xargs -0 -I{} -n1 -P "${BA_JOBS}" bash -c 'ingest_one "$@"' _ {}

# -------------------------------
# Summaries
# -------------------------------
echo "Run metadata:"
cat "${BA_METADIR}/ba_startfolder"

count_file_lines() { [[ -f "$1" ]] && wc -l < "$1" || echo 0; }
echo "$(count_file_lines "${BA_METADIR}/file_processed.txt") files in file_processed.txt"
echo "$(count_file_lines "${BA_METADIR}/file_skipped.txt")   files in file_skipped.txt"
echo "$(count_file_lines "${BA_METADIR}/file_ingested.txt")  files in file_ingested.txt"

# Cleanup
rm -f -- "$TMP_REF"

echo "Done. Meta: ${BA_METADIR}"