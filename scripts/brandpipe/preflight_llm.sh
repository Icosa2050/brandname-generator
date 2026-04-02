#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

CHECK_LMSTUDIO=0
CHECK_OLLAMA=0
REQUIRE_OPENROUTER=0
HTTP_TIMEOUT_S="${PREFLIGHT_HTTP_TIMEOUT_S:-4}"

LMSTUDIO_BASE_URL="${PREFLIGHT_LMSTUDIO_BASE_URL:-http://127.0.0.1:1234/v1}"
OLLAMA_BASE_URL="${PREFLIGHT_OLLAMA_BASE_URL:-http://127.0.0.1:11434}"
LMSTUDIO_MODELS_CSV="${PREFLIGHT_LMSTUDIO_MODELS:-}"
OLLAMA_MODELS_CSV="${PREFLIGHT_OLLAMA_MODELS:-}"

trim_csv_whitespace() {
  local raw="$1"
  local -a items=("${(@s:,:)raw}")
  local -a cleaned=()
  local item=""
  for item in "${items[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    [[ -n "$item" ]] && cleaned+=("$item")
  done
  if (( ${#cleaned[@]} == 0 )); then
    print -r -- ""
    return
  fi
  local IFS=','
  print -r -- "${cleaned[*]}"
}

lmstudio_models_url() {
  local url="${1%/}"
  if [[ "$url" == */models ]]; then
    print -r -- "$url"
    return
  fi
  if [[ "$url" == */v1 ]]; then
    print -r -- "${url}/models"
    return
  fi
  print -r -- "${url}/models"
}

ollama_tags_url() {
  local url="${1%/}"
  if [[ "$url" == */api/tags ]]; then
    print -r -- "$url"
    return
  fi
  if [[ "$url" == */v1 ]]; then
    print -r -- "${url%/v1}/api/tags"
    return
  fi
  if [[ "$url" == */api ]]; then
    print -r -- "${url}/tags"
    return
  fi
  print -r -- "${url}/api/tags"
}

has_openrouter_key() {
  if [[ -n "${OPENROUTER_API_KEY:-}" ]]; then
    return 0
  fi
  if command -v direnv >/dev/null 2>&1; then
    direnv exec "$ROOT_DIR" python3 -c 'import os,sys; sys.exit(0 if os.getenv("OPENROUTER_API_KEY") else 1)' >/dev/null 2>&1
    return $?
  fi
  return 1
}

check_lmstudio_endpoint() {
  local url
  url="$(lmstudio_models_url "$LMSTUDIO_BASE_URL")"
  local payload
  if ! payload="$(curl -fsS --max-time "$HTTP_TIMEOUT_S" "$url")"; then
    echo "PREFLIGHT_FAIL component=lmstudio reason=endpoint_unreachable url=$url timeout_s=$HTTP_TIMEOUT_S" >&2
    return 1
  fi
  if [[ -z "$LMSTUDIO_MODELS_CSV" ]]; then
    echo "PREFLIGHT_OK component=lmstudio check=endpoint url=$url"
    return 0
  fi
  if ! python3 - "$LMSTUDIO_MODELS_CSV" "$payload" <<'PY'; then
import json
import sys

required = [m.strip() for m in sys.argv[1].split(",") if m.strip()]
payload = json.loads(sys.argv[2])
available = {str(item.get("id", "")).strip() for item in payload.get("data", []) if isinstance(item, dict)}
missing = [model for model in required if model not in available]
if missing:
    print("missing=" + ",".join(missing))
    raise SystemExit(1)
print("matched=" + str(len(required)))
PY
    echo "PREFLIGHT_FAIL component=lmstudio reason=model_missing base_url=$LMSTUDIO_BASE_URL required=$LMSTUDIO_MODELS_CSV" >&2
    return 1
  fi
  echo "PREFLIGHT_OK component=lmstudio check=model_presence base_url=$LMSTUDIO_BASE_URL required=$LMSTUDIO_MODELS_CSV"
}

check_ollama_endpoint() {
  local url
  url="$(ollama_tags_url "$OLLAMA_BASE_URL")"
  local payload
  if ! payload="$(curl -fsS --max-time "$HTTP_TIMEOUT_S" "$url")"; then
    echo "PREFLIGHT_FAIL component=ollama reason=endpoint_unreachable url=$url timeout_s=$HTTP_TIMEOUT_S" >&2
    return 1
  fi
  if [[ -z "$OLLAMA_MODELS_CSV" ]]; then
    echo "PREFLIGHT_OK component=ollama check=endpoint url=$url"
    return 0
  fi
  if ! python3 - "$OLLAMA_MODELS_CSV" "$payload" <<'PY'; then
import json
import sys

required = [m.strip() for m in sys.argv[1].split(",") if m.strip()]
payload = json.loads(sys.argv[2])
available = {str(item.get("name", "")).strip() for item in payload.get("models", []) if isinstance(item, dict)}
def is_present(model: str) -> bool:
    if model in available:
        return True
    if ":" in model:
        return False
    prefix = model + ":"
    return any(name.startswith(prefix) for name in available)

missing = [model for model in required if not is_present(model)]
if missing:
    print("missing=" + ",".join(missing))
    raise SystemExit(1)
print("matched=" + str(len(required)))
PY
    echo "PREFLIGHT_FAIL component=ollama reason=model_missing base_url=$OLLAMA_BASE_URL required=$OLLAMA_MODELS_CSV" >&2
    return 1
  fi
  echo "PREFLIGHT_OK component=ollama check=model_presence base_url=$OLLAMA_BASE_URL required=$OLLAMA_MODELS_CSV"
}

usage() {
  cat <<'EOF'
Usage: scripts/brandpipe/preflight_llm.sh [options]

Checks local LLM endpoint/model availability and optional OpenRouter key presence.

Options:
  --check-lmstudio                 Validate LM Studio OpenAI-compatible endpoint
  --check-ollama                   Validate Ollama endpoint
  --lmstudio-base-url <url>        LM Studio base URL (default: http://127.0.0.1:1234/v1)
  --ollama-base-url <url>          Ollama base URL (default: http://127.0.0.1:11434)
  --lmstudio-model <id|csv>        Required LM Studio model id(s), comma-separated allowed
  --ollama-model <id|csv>          Required Ollama model name(s), comma-separated allowed
  --require-openrouter             Require OPENROUTER_API_KEY (env or direnv)
  --http-timeout-s <seconds>       Endpoint timeout per call (default: 4)
  -h, --help                       Show this help

Exit codes:
  0 if all requested checks pass
  2 for usage/configuration errors
  3 for failed runtime checks
EOF
}

require_arg_value() {
  local option="$1"
  local value="${2-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "Missing value for $option" >&2
    usage
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-lmstudio)
      CHECK_LMSTUDIO=1
      shift
      ;;
    --check-ollama)
      CHECK_OLLAMA=1
      shift
      ;;
    --lmstudio-base-url)
      require_arg_value "$1" "${2-}"
      LMSTUDIO_BASE_URL="$2"
      shift 2
      ;;
    --lmstudio-base-url=*)
      LMSTUDIO_BASE_URL="${1#*=}"
      shift
      ;;
    --ollama-base-url)
      require_arg_value "$1" "${2-}"
      OLLAMA_BASE_URL="$2"
      shift 2
      ;;
    --ollama-base-url=*)
      OLLAMA_BASE_URL="${1#*=}"
      shift
      ;;
    --lmstudio-model|--lmstudio-models)
      require_arg_value "$1" "${2-}"
      LMSTUDIO_MODELS_CSV="$2"
      shift 2
      ;;
    --lmstudio-model=*|--lmstudio-models=*)
      LMSTUDIO_MODELS_CSV="${1#*=}"
      shift
      ;;
    --ollama-model|--ollama-models)
      require_arg_value "$1" "${2-}"
      OLLAMA_MODELS_CSV="$2"
      shift 2
      ;;
    --ollama-model=*|--ollama-models=*)
      OLLAMA_MODELS_CSV="${1#*=}"
      shift
      ;;
    --require-openrouter)
      REQUIRE_OPENROUTER=1
      shift
      ;;
    --http-timeout-s)
      require_arg_value "$1" "${2-}"
      HTTP_TIMEOUT_S="$2"
      shift 2
      ;;
    --http-timeout-s=*)
      HTTP_TIMEOUT_S="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "$HTTP_TIMEOUT_S" != <-> ]] || (( HTTP_TIMEOUT_S <= 0 )); then
  echo "Invalid --http-timeout-s: $HTTP_TIMEOUT_S (expected positive integer)." >&2
  exit 2
fi

LMSTUDIO_MODELS_CSV="$(trim_csv_whitespace "$LMSTUDIO_MODELS_CSV")"
OLLAMA_MODELS_CSV="$(trim_csv_whitespace "$OLLAMA_MODELS_CSV")"

if (( REQUIRE_OPENROUTER )); then
  if ! has_openrouter_key; then
    echo "PREFLIGHT_FAIL component=openrouter reason=api_key_missing hint='set OPENROUTER_API_KEY or configure direnv/.envrc'" >&2
    exit 3
  fi
  echo "PREFLIGHT_OK component=openrouter check=api_key"
fi

if (( CHECK_LMSTUDIO )); then
  check_lmstudio_endpoint || exit 3
fi
if (( CHECK_OLLAMA )); then
  check_ollama_endpoint || exit 3
fi

if (( ! CHECK_LMSTUDIO && ! CHECK_OLLAMA && ! REQUIRE_OPENROUTER )); then
  echo "No checks requested. Use --check-lmstudio and/or --check-ollama and/or --require-openrouter." >&2
  exit 2
fi

echo "PREFLIGHT_OK summary=all_checks_passed"
