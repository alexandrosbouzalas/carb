#!/usr/bin/env bash
# install.sh — portable installer for carb (Linux & macOS)
# Usage: bash install.sh
set -Eeuo pipefail

# --- Settings ---------------------------------------------------------------
PREFIX="${PREFIX:-}"          # optional override: PREFIX=/opt/mytools/carb bash install.sh
BIN_NAME="carb"
SRC_BIN="carb.sh"
SRC_MAN="carb.1"

# --- Locate sources ---------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SRC_BIN_PATH="${SCRIPT_DIR}/${SRC_BIN}"
SRC_MAN_PATH="${SCRIPT_DIR}/${SRC_MAN}"

if [ ! -f "$SRC_BIN_PATH" ]; then echo "ERROR: $SRC_BIN not found at $SRC_BIN_PATH"; exit 1; fi
if [ ! -f "$SRC_MAN_PATH" ]; then echo "ERROR: $SRC_MAN not found at $SRC_MAN_PATH"; exit 1; fi

# --- Detect OS / default prefix --------------------------------------------
OS="$(uname -s || echo Unknown)"
case "$OS" in
  Darwin)
    # Always prefer a dedicated /carb subfolder under the usual roots.
    if [ -d /opt/homebrew ]; then
      DEFAULT_PREFIX="/opt/homebrew/carb"
    else
      DEFAULT_PREFIX="/usr/local/carb"
    fi
    ;;
  Linux|*BSD*)
    DEFAULT_PREFIX="/usr/local/carb"
    ;;
  *)
    DEFAULT_PREFIX="/usr/local/carb"
    echo "WARN: Unrecognized OS '$OS'. Using $DEFAULT_PREFIX."
    ;;
esac

# Allow user override via PREFIX env, otherwise use default
PREFIX="${PREFIX:-$DEFAULT_PREFIX}"

DEST_BIN_DIR="${PREFIX}/bin"
DEST_MAN_DIR="${PREFIX}/share/man/man1"
DEST_BIN="${DEST_BIN_DIR}/${BIN_NAME}"
DEST_MAN="${DEST_MAN_DIR}/${BIN_NAME}.1"

# Per-user fallback
USER_BIN_DIR="${HOME}/.local/bin"
USER_MAN_DIR="${HOME}/.local/share/man/man1"
USER_BIN="${USER_BIN_DIR}/${BIN_NAME}"
USER_MAN="${USER_MAN_DIR}/${BIN_NAME}.1"

# --- Helpers ----------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }

is_writable_dir() {
  local d="$1"
  [ -d "$d" ] && [ -w "$d" ]
}

ensure_dir() {
  local d="$1" use_sudo="$2"
  if [ "$use_sudo" = "sudo" ]; then sudo mkdir -p "$d"; else mkdir -p "$d"; fi
}

install_file() {
  local mode="$1" src="$2" dst="$3" use_sudo="$4"
  if [ "$use_sudo" = "sudo" ]; then sudo install -m "$mode" "$src" "$dst"; else install -m "$mode" "$src" "$dst"; fi
}

refresh_man_db() {
  # Linux: mandb; macOS: makewhatis
  local use_sudo="$1"
  if have mandb; then
    if [ "$use_sudo" = "sudo" ]; then sudo mandb || true; else mandb || true; fi
  elif [ "$OS" = "Darwin" ] && [ -x /usr/libexec/makewhatis ]; then
    # Rebuild only this prefix's man root to keep it quick
    local man_root
    man_root="$(dirname "$(dirname "$DEST_MAN_DIR")")"  # .../share/man
    if [ "$use_sudo" = "sudo" ]; then sudo /usr/libexec/makewhatis "$man_root" || true
    else /usr/libexec/makewhatis "$man_root" || true
    fi
  fi
}

# --- Decide system-wide vs per-user -----------------------------------------
echo "==> Installing to prefix: $PREFIX"
echo "    bin: $DEST_BIN_DIR"
echo "    man: $DEST_MAN_DIR"
echo "    note: backups produced by 'carb' will live under this tree (next to the binary)."

# Determine writability of target dirs or their parents
BIN_DIR_WRITABLE="no"; is_writable_dir "$DEST_BIN_DIR" && BIN_DIR_WRITABLE="yes"
MAN_DIR_WRITABLE="no"; is_writable_dir "$DEST_MAN_DIR" && MAN_DIR_WRITABLE="yes"

PREFIX_WRITABLE="no"; [ -w "$PREFIX" ] 2>/dev/null && PREFIX_WRITABLE="yes"
BIN_PARENT_WRITABLE="no"; [ -w "$(dirname "$DEST_BIN_DIR")" ] 2>/dev/null && BIN_PARENT_WRITABLE="yes"
MAN_PARENT_WRITABLE="no"; [ -w "$(dirname "$DEST_MAN_DIR")" ] 2>/dev/null && MAN_PARENT_WRITABLE="yes"

USE_SUDO=""
if [ "$BIN_DIR_WRITABLE" = "yes" ] && [ "$MAN_DIR_WRITABLE" = "yes" ]; then
  USE_SUDO=""
elif have sudo; then
  USE_SUDO="sudo"
fi

CAN_SYSTEM="no"
if [ -n "$USE_SUDO" ]; then
  CAN_SYSTEM="yes"
else
  # No sudo: only proceed system-wide if dirs (or their parents) are writable
  if [ "$BIN_DIR_WRITABLE" = "yes" ] && [ "$MAN_DIR_WRITABLE" = "yes" ]; then
    CAN_SYSTEM="yes"
  elif [ "$PREFIX_WRITABLE" = "yes" ] || [ "$BIN_PARENT_WRITABLE" = "yes" ] || [ "$MAN_PARENT_WRITABLE" = "yes" ]; then
    CAN_SYSTEM="yes"
  fi
fi

if [ "$CAN_SYSTEM" = "yes" ]; then
  # --- System-wide install ---------------------------------------------------
  ensure_dir "$DEST_BIN_DIR" "$USE_SUDO"
  ensure_dir "$DEST_MAN_DIR" "$USE_SUDO"

  echo "==> Installing executable"
  install_file 0755 "$SRC_BIN_PATH" "$DEST_BIN" "$USE_SUDO"

  echo "==> Installing manpage"
  install_file 0644 "$SRC_MAN_PATH" "$DEST_MAN" "$USE_SUDO"

  echo "==> Refreshing man database"
  refresh_man_db "$USE_SUDO"

  echo "✅ Installed system-wide:"
  echo "   $DEST_BIN"
  echo "   $DEST_MAN"
else
  # --- Per-user fallback -----------------------------------------------------
  echo "==> Falling back to per-user install (no sudo or unwritable prefix)"
  mkdir -p "$USER_BIN_DIR" "$USER_MAN_DIR"
  install -m 0755 "$SRC_BIN_PATH" "$USER_BIN"
  install -m 0644 "$SRC_MAN_PATH" "$USER_MAN"

  echo "==> Attempting to refresh per-user man db (may be a no-op)"
  if [ "$OS" = "Darwin" ] && [ -x /usr/libexec/makewhatis ]; then
    /usr/libexec/makewhatis "$HOME/.local/share/man" || true
  elif have mandb; then
    mandb || true
  fi

  echo "✅ Installed for current user:"
  echo "   $USER_BIN"
  echo "   $USER_MAN"

  case ":$PATH:" in *":$USER_BIN_DIR:"*) ;; *)
    echo
    echo "Add to your shell rc if 'carb' is not found:"
    echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
  esac

  if [ -n "${MANPATH:-}" ]; then
    case ":$MANPATH:" in *":$HOME/.local/share/man:"*) ;; *)
      echo
      echo "Add to your shell rc if 'man carb' is not found:"
      echo "  export MANPATH=\"\$HOME/.local/share/man:\${MANPATH:-}\""
    esac
  fi
fi

# --- Verify ------------------------------------------------------------------
echo
echo "==> Verifying installation"
if have "$BIN_NAME"; then
  "$BIN_NAME" --version || true
  echo "Run 'man carb' to view the manpage."
else
  echo "WARN: '$BIN_NAME' not yet on PATH. Open a new shell or update PATH."
fi