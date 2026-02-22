# brandname-generator

Standalone Python naming/branding pipeline with:
- candidate generation
- LLM ideation (OpenRouter)
- async validation
- exclusion memory (SQLite) to avoid re-validating eliminated names

## Quickstart
1. Set env: OPENROUTER_API_KEY, OPENROUTER_HTTP_REFERER, OPENROUTER_X_TITLE
2. Run tuned lane 0:
   scripts/branding/run_openrouter_lane.sh --lane 0 --out-dir /tmp/branding_openrouter_tuned
3. Run tuned lane 1:
   scripts/branding/run_openrouter_lane.sh --lane 1 --out-dir /tmp/branding_openrouter_tuned
4. See full runner flags:
   python3 scripts/branding/naming_campaign_runner.py --help
