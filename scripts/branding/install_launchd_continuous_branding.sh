#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SUPERVISOR_SCRIPT="$ROOT_DIR/scripts/branding/run_continuous_branding_supervisor.sh"

LABEL="${LAUNCHD_LABEL:-com.icosa.brandname_generator.continuous}"
PLIST_PATH="${LAUNCHD_PLIST_PATH:-$HOME/Library/LaunchAgents/${LABEL}.plist}"
LOG_DIR="${LAUNCHD_LOG_DIR:-$HOME/Library/Logs/brandname-generator}"

OUT_DIR="${LAUNCHD_OUT_DIR:-$ROOT_DIR/test_outputs/branding/continuous_hybrid}"
BACKEND="${LAUNCHD_BACKEND:-auto}"
FALLBACK_BACKEND="${LAUNCHD_FALLBACK_BACKEND:-ollama}"
PROFILE_PLAN="${LAUNCHD_PROFILE_PLAN:-fast,fast,quality}"
TARGET_GOOD="${LAUNCHD_TARGET_GOOD:-120}"
TARGET_STRONG="${LAUNCHD_TARGET_STRONG:-40}"
MAX_CYCLES="${LAUNCHD_MAX_CYCLES:-0}"
SLEEP_OK_S="${LAUNCHD_SLEEP_OK_S:-20}"
SLEEP_FAIL_BASE_S="${LAUNCHD_SLEEP_FAIL_BASE_S:-30}"
SLEEP_FAIL_MAX_S="${LAUNCHD_SLEEP_FAIL_MAX_S:-300}"
MAX_FAIL_STREAK="${LAUNCHD_MAX_FAIL_STREAK:-12}"
HEALTHCHECK=1

ACTION="install"  # install|uninstall|status|print
EXTRA_SUPERVISOR_ARGS=()

xml_escape() {
  print -r -- "$1" \
    | sed -e 's/&/\&amp;/g' \
          -e 's/</\&lt;/g' \
          -e 's/>/\&gt;/g' \
          -e 's/"/\&quot;/g' \
          -e "s/'/\&apos;/g"
}

usage() {
  cat <<'EOF'
Install/manage a macOS LaunchAgent for continuous branding supervision.

Usage:
  scripts/branding/install_launchd_continuous_branding.sh [action] [options] [-- <extra supervisor args>]

Actions:
  --install      Write plist and start service (default)
  --uninstall    Stop service and remove plist
  --status       Show launchctl status
  --print        Print generated plist to stdout (no install)

Options:
  --label <name>                   LaunchAgent label
  --plist-path <path>              Destination plist path
  --log-dir <path>                 Directory for stdout/stderr logs
  --out-dir <path>                 Supervisor --out-dir
  --backend <auto|lmstudio|ollama> Supervisor backend (default: auto)
  --fallback-backend <none|lmstudio|ollama>
  --profile-plan <csv>             fast,fast,quality (default)
  --target-good <n>                Stop threshold for strict strong+consider
  --target-strong <n>              Stop threshold for strict strong
  --max-cycles <n>                 0 => unlimited
  --sleep-ok-s <seconds>
  --sleep-fail-base-s <seconds>
  --sleep-fail-max-s <seconds>
  --max-fail-streak <n>
  --no-healthcheck
  -h, --help

Examples:
  scripts/branding/install_launchd_continuous_branding.sh --install
  scripts/branding/install_launchd_continuous_branding.sh --status
  scripts/branding/install_launchd_continuous_branding.sh --uninstall
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install)
      ACTION="install"
      shift
      ;;
    --uninstall)
      ACTION="uninstall"
      shift
      ;;
    --status)
      ACTION="status"
      shift
      ;;
    --print)
      ACTION="print"
      shift
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --plist-path)
      PLIST_PATH="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --backend)
      BACKEND="$2"
      shift 2
      ;;
    --fallback-backend)
      FALLBACK_BACKEND="$2"
      shift 2
      ;;
    --profile-plan)
      PROFILE_PLAN="$2"
      shift 2
      ;;
    --target-good)
      TARGET_GOOD="$2"
      shift 2
      ;;
    --target-strong)
      TARGET_STRONG="$2"
      shift 2
      ;;
    --max-cycles)
      MAX_CYCLES="$2"
      shift 2
      ;;
    --sleep-ok-s)
      SLEEP_OK_S="$2"
      shift 2
      ;;
    --sleep-fail-base-s)
      SLEEP_FAIL_BASE_S="$2"
      shift 2
      ;;
    --sleep-fail-max-s)
      SLEEP_FAIL_MAX_S="$2"
      shift 2
      ;;
    --max-fail-streak)
      MAX_FAIL_STREAK="$2"
      shift 2
      ;;
    --no-healthcheck)
      HEALTHCHECK=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_SUPERVISOR_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

typeset -a PROGRAM_ARGS=(
  "/bin/zsh"
  "$SUPERVISOR_SCRIPT"
  "--out-dir" "$OUT_DIR"
  "--backend" "$BACKEND"
  "--fallback-backend" "$FALLBACK_BACKEND"
  "--profile-plan" "$PROFILE_PLAN"
  "--target-good" "$TARGET_GOOD"
  "--target-strong" "$TARGET_STRONG"
  "--max-cycles" "$MAX_CYCLES"
  "--sleep-ok-s" "$SLEEP_OK_S"
  "--sleep-fail-base-s" "$SLEEP_FAIL_BASE_S"
  "--sleep-fail-max-s" "$SLEEP_FAIL_MAX_S"
  "--max-fail-streak" "$MAX_FAIL_STREAK"
)
if (( ! HEALTHCHECK )); then
  PROGRAM_ARGS+=(--no-healthcheck)
fi
if (( ${#EXTRA_SUPERVISOR_ARGS[@]} > 0 )); then
  PROGRAM_ARGS+=(-- "${EXTRA_SUPERVISOR_ARGS[@]}")
fi

render_plist() {
  local stdout_log="$LOG_DIR/${LABEL}.out.log"
  local stderr_log="$LOG_DIR/${LABEL}.err.log"

  cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$(xml_escape "$LABEL")</string>
  <key>ProgramArguments</key>
  <array>
EOF
  local arg
  for arg in "${PROGRAM_ARGS[@]}"; do
    echo "    <string>$(xml_escape "$arg")</string>"
  done
  cat <<EOF
  </array>
  <key>WorkingDirectory</key>
  <string>$(xml_escape "$ROOT_DIR")</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>$(xml_escape "$stdout_log")</string>
  <key>StandardErrorPath</key>
  <string>$(xml_escape "$stderr_log")</string>
</dict>
</plist>
EOF
}

domain="gui/$(id -u)"

case "$ACTION" in
  print)
    render_plist
    ;;
  status)
    if launchctl print "${domain}/${LABEL}" >/dev/null 2>&1; then
      launchctl print "${domain}/${LABEL}"
    else
      echo "LaunchAgent not loaded: ${domain}/${LABEL}"
      if [[ -f "$PLIST_PATH" ]]; then
        echo "Plist exists at: $PLIST_PATH"
      fi
      exit 1
    fi
    ;;
  uninstall)
    launchctl bootout "$domain" "$PLIST_PATH" >/dev/null 2>&1 || true
    rm -f "$PLIST_PATH"
    echo "Uninstalled LaunchAgent: $LABEL"
    ;;
  install)
    mkdir -p "$(dirname "$PLIST_PATH")"
    mkdir -p "$LOG_DIR"
    render_plist > "$PLIST_PATH"
    launchctl bootout "$domain" "$PLIST_PATH" >/dev/null 2>&1 || true
    launchctl bootstrap "$domain" "$PLIST_PATH"
    launchctl enable "${domain}/${LABEL}" >/dev/null 2>&1 || true
    launchctl kickstart -k "${domain}/${LABEL}"
    echo "Installed and started LaunchAgent: $LABEL"
    echo "Plist: $PLIST_PATH"
    echo "Logs:  $LOG_DIR"
    ;;
  *)
    echo "Unsupported action: $ACTION" >&2
    exit 2
    ;;
esac
