#!/usr/bin/env bash
# If launched as `zsh script.sh`, re-exec under bash for bash-specific syntax.
if [[ -z "${BASH_VERSION:-}" ]]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_SELF="$0"
if [[ -n "${BASH_SOURCE:-}" ]]; then
  SCRIPT_SELF="${BASH_SOURCE[0]}"
fi
SCRIPT_SELF="$(cd "$(dirname "$SCRIPT_SELF")" && pwd)/$(basename "$SCRIPT_SELF")"

# Idempotent setup for self-hosted macOS GitHub runners:
# - copies an existing Android SDK to a shared machine path
# - configures runner .env/.path to use the shared SDK
# - optionally restarts runner services
#
# Default targets:
#   /Users/github-runner/actions-runner-m4-01
#   /Users/github-runner/actions-runner-m4-02

SOURCE_SDK="/Users/bernhard/Library/Android/sdk"
DEST_SDK="/opt/android-sdk"
RUNNER_USER="github-runner"
RUNNER_GROUP="staff"
RUNNER_DIRS="/Users/github-runner/actions-runner-m4-01,/Users/github-runner/actions-runner-m4-02"
RESTART_RUNNERS="1"
COPY_PROGRESS="auto"
RSYNC_BIN=""

usage() {
  cat <<'EOF'
Usage:
  setup_shared_android_sdk_for_runners.sh [options]

Options:
  --source-sdk <path>    Source SDK path (default: /Users/bernhard/Library/Android/sdk)
  --dest-sdk <path>      Shared destination SDK path (default: /opt/android-sdk)
  --runner-user <name>   Runner user (default: github-runner)
  --runner-group <name>  Runner group (default: staff)
  --runner-dirs <csv>    Comma-separated runner directories
                         (default: /Users/github-runner/actions-runner-m4-01,/Users/github-runner/actions-runner-m4-02)
  --copy-progress        Force rsync progress output
  --no-copy-progress     Disable rsync progress output
  --no-restart           Do not restart runner services
  -h, --help             Show this help

Example:
  ./setup_shared_android_sdk_for_runners.sh \
    --source-sdk /Users/bernhard/Library/Android/sdk \
    --dest-sdk /opt/android-sdk
EOF
}

log() {
  printf '[setup-android-sdk] %s\n' "$*"
}

die() {
  printf '[setup-android-sdk] ERROR: %s\n' "$*" >&2
  exit 1
}

as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

ensure_privileges() {
  if [[ "$(id -u)" -eq 0 ]]; then
    return
  fi

  # Non-interactive shells cannot answer sudo prompts.
  if [[ ! -t 0 || ! -t 1 ]]; then
    die "Root privileges required. Re-run interactively with: sudo bash $SCRIPT_SELF [args]"
  fi

  if sudo -n true 2>/dev/null; then
    return
  fi

  log "Requesting sudo privileges..."
  exec sudo /usr/bin/env bash "$SCRIPT_SELF" "$@"
}

as_runner_shell() {
  local cmd="$1"
  if [[ "$(id -un)" == "$RUNNER_USER" ]]; then
    bash -lc "$cmd"
  else
    sudo -u "$RUNNER_USER" bash -lc "$cmd"
  fi
}

find_launchdaemon_label_for_runner() {
  local runner_dir="$1"
  local plist
  for plist in /Library/LaunchDaemons/*.plist; do
    [[ -f "$plist" ]] || continue
    if grep -Fq "$runner_dir/runsvc.sh" "$plist" || grep -Fq "$runner_dir" "$plist"; then
      basename "$plist" .plist
      return 0
    fi
  done
  return 1
}

restart_runner() {
  local runner_dir="$1"
  local svc_file="$runner_dir/svc.sh"
  local service_marker="$runner_dir/.service"
  local label=""

  # Standard GitHub runner install path: user LaunchAgent managed by svc.sh.
  if [[ -f "$service_marker" && -x "$svc_file" ]]; then
    log "Restarting runner via svc.sh in $runner_dir"
    as_runner_shell "cd '$runner_dir' && ./svc.sh stop || true"
    as_runner_shell "cd '$runner_dir' && ./svc.sh start"
    return 0
  fi

  # Fallback for custom system-level LaunchDaemon setups.
  if label="$(find_launchdaemon_label_for_runner "$runner_dir")"; then
    log "Restarting runner via launchctl label system/$label"
    as_root launchctl kickstart -k "system/$label"
    return 0
  fi

  log "WARNING: Could not determine service manager for $runner_dir; restart skipped."
  return 1
}

contains_entry() {
  local needle="$1"
  shift
  local candidate
  for candidate in "$@"; do
    [[ "$candidate" == "$needle" ]] && return 0
  done
  return 1
}

upsert_env_key() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp
  tmp="$(mktemp)"
  if [[ -f "$file" ]]; then
    awk -v k="$key" -v v="$value" '
      BEGIN { found = 0 }
      $0 ~ ("^" k "=") { print k "=" v; found = 1; next }
      { print }
      END { if (!found) print k "=" v }
    ' "$file" >"$tmp"
  else
    printf '%s=%s\n' "$key" "$value" >"$tmp"
  fi
  as_root install -m 0644 "$tmp" "$file"
  as_root chown "$RUNNER_USER:$RUNNER_GROUP" "$file"
  rm -f "$tmp"
}

rewrite_runner_path_file() {
  local file="$1"
  local existing_path
  if [[ -f "$file" ]]; then
    existing_path="$(tr '\n' ':' <"$file")"
    existing_path="${existing_path%:}"
  else
    existing_path="$PATH"
  fi

  local entries=()
  local filtered=()
  local required=()
  local entry
  IFS=':' read -r -a entries <<<"$existing_path"

  for entry in "${entries[@]}"; do
    [[ -n "$entry" ]] || continue
    case "$entry" in
      "$SOURCE_SDK"|"${SOURCE_SDK}/"*) continue ;;
    esac
    if ! contains_entry "$entry" "${filtered[@]}"; then
      filtered+=("$entry")
    fi
  done

  if [[ -d "$DEST_SDK/emulator" ]]; then
    required+=("$DEST_SDK/emulator")
  fi
  if [[ -d "$DEST_SDK/tools" ]]; then
    required+=("$DEST_SDK/tools")
  fi
  if [[ -d "$DEST_SDK/tools/bin" ]]; then
    required+=("$DEST_SDK/tools/bin")
  fi
  if [[ -d "$DEST_SDK/cmdline-tools/latest/bin" ]]; then
    required+=("$DEST_SDK/cmdline-tools/latest/bin")
  fi
  if [[ -d "$DEST_SDK/platform-tools" ]]; then
    required+=("$DEST_SDK/platform-tools")
  fi

  for entry in "${required[@]}"; do
    if ! contains_entry "$entry" "${filtered[@]}"; then
      filtered+=("$entry")
    fi
  done

  local new_path
  local tmp
  tmp="$(mktemp)"
  IFS=':' new_path="${filtered[*]}"
  printf '%s\n' "$new_path" >"$tmp"
  as_root install -m 0644 "$tmp" "$file"
  as_root chown "$RUNNER_USER:$RUNNER_GROUP" "$file"
  rm -f "$tmp"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --source-sdk)
        SOURCE_SDK="${2:-}"
        shift 2
        ;;
      --dest-sdk)
        DEST_SDK="${2:-}"
        shift 2
        ;;
      --runner-user)
        RUNNER_USER="${2:-}"
        shift 2
        ;;
      --runner-group)
        RUNNER_GROUP="${2:-}"
        shift 2
        ;;
      --runner-dirs)
        RUNNER_DIRS="${2:-}"
        shift 2
        ;;
      --copy-progress)
        COPY_PROGRESS="1"
        shift
        ;;
      --no-copy-progress)
        COPY_PROGRESS="0"
        shift
        ;;
      --no-restart)
        RESTART_RUNNERS="0"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done
}

copy_shared_sdk() {
  local -a rsync_args
  rsync_args=(-a)
  local -a progress_args
  progress_args=(--progress)

  # Apple-provided rsync (2.6.9/openrsync) does not support --info=progress2.
  if "$RSYNC_BIN" --help 2>&1 | grep -q -- '--info'; then
    progress_args=(--info=progress2)
  fi

  case "$COPY_PROGRESS" in
    auto)
      if [[ -t 1 ]]; then
        rsync_args+=("${progress_args[@]}")
      fi
      ;;
    1)
      rsync_args+=("${progress_args[@]}")
      ;;
    0)
      ;;
    *)
      die "Invalid COPY_PROGRESS value: $COPY_PROGRESS"
      ;;
  esac

  [[ -d "$SOURCE_SDK" ]] || die "Source SDK not found: $SOURCE_SDK"
  log "Syncing SDK from $SOURCE_SDK to $DEST_SDK"
  log "Using rsync binary: $RSYNC_BIN"
  log "rsync version: $("$RSYNC_BIN" --version | head -n 1)"
  log "Copy can take several minutes on first run."
  as_root mkdir -p "$DEST_SDK"
  as_root "$RSYNC_BIN" "${rsync_args[@]}" "$SOURCE_SDK/" "$DEST_SDK/"
  as_root chown -R "$RUNNER_USER:$RUNNER_GROUP" "$DEST_SDK"
  # Read + execute for all; write remains owner-controlled.
  as_root chmod -R a+rX "$DEST_SDK"
}

configure_runner_dir() {
  local runner_dir="$1"
  local env_file="$runner_dir/.env"
  local path_file="$runner_dir/.path"
  local svc_file="$runner_dir/svc.sh"

  [[ -d "$runner_dir" ]] || die "Runner directory not found: $runner_dir"
  [[ -x "$svc_file" ]] || die "Missing runner service script: $svc_file"

  log "Configuring runner: $runner_dir"
  upsert_env_key "$env_file" "ANDROID_HOME" "$DEST_SDK"
  upsert_env_key "$env_file" "ANDROID_SDK_ROOT" "$DEST_SDK"
  rewrite_runner_path_file "$path_file"

  if [[ "$RESTART_RUNNERS" == "1" ]]; then
    restart_runner "$runner_dir" || true
  fi
}

validate() {
  log "Validation:"
  if [[ -x "$DEST_SDK/platform-tools/adb" ]]; then
    as_runner_shell "'$DEST_SDK/platform-tools/adb' version | head -n 1"
  else
    die "adb not found at $DEST_SDK/platform-tools/adb"
  fi

  if as_runner_shell "command -v flutter >/dev/null 2>&1"; then
    as_runner_shell "ANDROID_HOME='$DEST_SDK' ANDROID_SDK_ROOT='$DEST_SDK' flutter doctor -v | sed -n '/Android toolchain/,+7p'"
  else
    log "flutter is not on base runner PATH (this is fine if workflows use subosito/flutter-action)."
  fi
}

main() {
  parse_args "$@"
  if [[ -x "/opt/homebrew/bin/rsync" ]]; then
    RSYNC_BIN="/opt/homebrew/bin/rsync"
  elif command -v rsync >/dev/null 2>&1; then
    RSYNC_BIN="$(command -v rsync)"
  else
    die "rsync not found on PATH"
  fi
  ensure_privileges "$@"
  copy_shared_sdk

  local runner
  IFS=',' read -r -a runner_list <<<"$RUNNER_DIRS"
  for runner in "${runner_list[@]}"; do
    configure_runner_dir "$runner"
  done

  validate
  log "Done."
}

main "$@"
