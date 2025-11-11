#!/usr/bin/env bash
# uninstall.sh — portable uninstaller for carb (Linux & macOS)
# Usage: bash uninstall.sh [--system|--user] [--purge] [--dry-run] [--no-refresh]
# Env overrides: BIN_NAME=carb PREFIX=/custom/prefix
set -Eeuo pipefail

# --- Settings ---------------------------------------------------------------
BIN_NAME="${BIN_NAME:-carb}"
OS="$(uname -s || echo Unknown)"
DRY_RUN="no"
PURGE_EMPTY_DIRS="no"
REFRESH_DB="yes"
SCOPE=""        # "", "system", or "user"
PREFIX="${PREFIX:-}"

# --- Parse args -------------------------------------------------------------
while (($#)); do
  case "$1" in
    --system) SCOPE="system" ;;
    --user)   SCOPE="user" ;;
    --purge)  PURGE_EMPTY_DIRS="yes" ;;
    --dry-run) DRY_RUN="yes" ;;
    --no-refresh) REFRESH_DB="no" ;;
    -h|--help)
      cat <<EOF
uninstall.sh — remove ${BIN_NAME} and its manpage.

Usage:
  bash uninstall.sh [--system|--user] [--purge] [--dry-run] [--no-refresh]

Options:
  --system       Uninstall from system prefix (uses sudo if required).
  --user         Uninstall from per-user location (~/.local).
  --purge        Also remove now-empty bin/man directories under the chosen scope.
  --dry-run      Show what would be removed, but do nothing.
  --no-refresh   Skip refreshing man database after uninstall.

Environment:
  BIN_NAME       Program name to uninstall (default: ${BIN_NAME})
  PREFIX         Override system prefix detection (e.g. /usr/local, /opt/homebrew)

Notes:
  If neither --system nor --user is given, the script tries both (system first).
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

# --- Helpers ----------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }

say() { printf '%s\n' "$*"; }
do_run() {
  if [ "$DRY_RUN" = "yes" ]; then
    say "[dry-run] $*"
  else
    eval "$@"
  fi
}

is_writable_dir() {
  local d="$1"
  [ -d "$d" ] && [ -w "$d" ]
}

rm_file() {
  # rm -f <path> (sudo if needed)
  local path="$1" use_sudo="$2"
  if [ -e "$path" ]; then
    if [ "$use_sudo" = "sudo" ]; then do_run sudo rm -f -- "$path"
    else do_run rm -f -- "$path"
    fi
  fi
}

maybe_rmdir() {
  # remove dir if empty (sudo if needed)
  local d="$1" use_sudo="$2"
  if [ "$PURGE_EMPTY_DIRS" = "yes" ] && [ -d "$d" ]; then
    # Only remove if empty
    if [ -z "$(ls -A "$d" 2>/dev/null || true)" ]; then
      if [ "$use_sudo" = "sudo" ]; then do_run sudo rmdir -- "$d"
      else do_run rmdir -- "$d"
      fi
    fi
  fi
}

refresh_man_db() {
  local use_sudo="$1" man_root="$2"
  [ "$REFRESH_DB" = "yes" ] || return 0
  if have mandb; then
    if [ "$use_sudo" = "sudo" ]; then do_run "sudo mandb || true"
    else do_run "mandb || true"
    fi
  elif [ "$OS" = "Darwin" ] && [ -x /usr/libexec/makewhatis ]; then
    # Limit to the relevant man root to keep it fast
    if [ -n "$man_root" ] && [ -d "$man_root" ]; then
      if [ "$use_sudo" = "sudo" ]; then do_run "sudo /usr/libexec/makewhatis \"$man_root\" || true"
      else do_run "/usr/libexec/makewhatis \"$man_root\" || true"
      fi
    fi
  fi
}

# --- Detect default PREFIX (mirrors install.sh) ------------------------------
if [ -z "$PREFIX" ]; then
  case "$OS" in
    Darwin)
      if have brew; then
        PREFIX="$(brew --prefix 2>/dev/null || echo /usr/local)"
      else
        if [ -d /opt/homebrew ]; then PREFIX="/opt/homebrew"; else PREFIX="/usr/local"; fi
      fi
      ;;
    Linux|*BSD*) PREFIX="/usr/local" ;;
    *) PREFIX="/usr/local"; say "WARN: Unrecognized OS '$OS'. Using $PREFIX." ;;
  esac
fi

# --- Paths (system) ----------------------------------------------------------
DEST_BIN_DIR="${PREFIX}/bin"
DEST_MAN_DIR="${PREFIX}/share/man/man1"
DEST_BIN="${DEST_BIN_DIR}/${BIN_NAME}"
DEST_MAN="${DEST_MAN_DIR}/${BIN_NAME}.1"

# --- Paths (user) ------------------------------------------------------------
USER_BIN_DIR="${HOME}/.local/bin"
USER_MAN_DIR="${HOME}/.local/share/man/man1"
USER_BIN="${USER_BIN_DIR}/${BIN_NAME}"
USER_MAN="${USER_MAN_DIR}/${BIN_NAME}.1"

# --- Decide sudo for system scope -------------------------------------------
USE_SUDO=""
BIN_DIR_WRITABLE="no"; is_writable_dir "$DEST_BIN_DIR" && BIN_DIR_WRITABLE="yes"
MAN_DIR_WRITABLE="no"; is_writable_dir "$DEST_MAN_DIR" && MAN_DIR_WRITABLE="yes"
PREFIX_WRITABLE="no"; [ -w "$PREFIX" ] 2>/dev/null && PREFIX_WRITABLE="yes"

if [ "$BIN_DIR_WRITABLE" = "yes" ] && [ "$MAN_DIR_WRITABLE" = "yes" ] && [ "$PREFIX_WRITABLE" = "yes" ]; then
  USE_SUDO=""
elif have sudo; then
  USE_SUDO="sudo"
fi

# --- Uninstall routines ------------------------------------------------------
uninstall_system() {
  say "==> Uninstalling (system prefix): $PREFIX"
  say "    removing: $DEST_BIN"
  rm_file "$DEST_BIN" "$USE_SUDO"

  say "    removing: $DEST_MAN"
  rm_file "$DEST_MAN" "$USE_SUDO"

  # Optionally clean empty man/bin leaf dirs
  if [ "$PURGE_EMPTY_DIRS" = "yes" ]; then
    maybe_rmdir "$DEST_MAN_DIR" "$USE_SUDO"
    maybe_rmdir "$(dirname "$DEST_MAN_DIR")" "$USE_SUDO"      # .../share/man
    maybe_rmdir "$(dirname "$(dirname "$DEST_MAN_DIR")")" "$USE_SUDO"  # .../share
    maybe_rmdir "$DEST_BIN_DIR" "$USE_SUDO"
  fi

  # Refresh man database, limiting to this prefix's man root if available
  local man_root
  man_root="$(dirname "$(dirname "$DEST_MAN_DIR")")"  # .../share/man
  refresh_man_db "$USE_SUDO" "$man_root"
}

uninstall_user() {
  say "==> Uninstalling (per-user): $HOME/.local"
  say "    removing: $USER_BIN"
  rm_file "$USER_BIN" ""

  say "    removing: $USER_MAN"
  rm_file "$USER_MAN" ""

  if [ "$PURGE_EMPTY_DIRS" = "yes" ]; then
    maybe_rmdir "$USER_MAN_DIR" ""
    maybe_rmdir "$(dirname "$USER_MAN_DIR")" ""       # ~/.local/share/man
    maybe_rmdir "$(dirname "$(dirname "$USER_MAN_DIR")")" "" # ~/.local/share
    maybe_rmdir "$USER_BIN_DIR" ""
  fi

  # Try to refresh man db for user space (may be a no-op)
  local man_root="$HOME/.local/share/man"
  refresh_man_db "" "$man_root"
}

# --- Decide scope and act ----------------------------------------------------
ANY_REMOVED="no"

run_system() {
  # Only try if paths exist or explicit scope
  if [ -e "$DEST_BIN" ] || [ -e "$DEST_MAN" ] || [ "$SCOPE" = "system" ]; then
    uninstall_system; ANY_REMOVED="yes"
  fi
}

run_user() {
  if [ -e "$USER_BIN" ] || [ -e "$USER_MAN" ] || [ "$SCOPE" = "user" ]; then
    uninstall_user; ANY_REMOVED="yes"
  fi
}

case "$SCOPE" in
  system) run_system ;;
  user)   run_user ;;
  "")
    # Try system first, then user
    run_system
    run_user
    ;;
esac

say
if [ "$ANY_REMOVED" = "yes" ]; then
  say "✅ Uninstall finished${DRY_RUN:+ (dry-run)}."
else
  say "Nothing to remove. (${BIN_NAME} not found in system prefix '$PREFIX' or user paths.)"
fi