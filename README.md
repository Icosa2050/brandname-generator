# brandname-generator

Standalone Python naming/branding pipeline with:
- candidate generation
- LLM ideation (OpenRouter)
- async validation
- exclusion memory (SQLite) to avoid re-validating eliminated names

## Quickstart
1. Set env: OPENROUTER_API_KEY, OPENROUTER_HTTP_REFERER, OPENROUTER_X_TITLE
2. Run campaign:
   python3 scripts/branding/naming_campaign_runner.py --help
