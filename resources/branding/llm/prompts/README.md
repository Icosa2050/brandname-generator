# Prompt Variants (brand + market)

Store custom prompt variants here, one file per brand/market run profile.

Recommended naming:
- `<brand_slug>/<market_slug>.txt`

Examples:
- `settlementapp/dach_it.txt`
- `settlementapp/eu_en.txt`
- `propertyops/global_en.txt`

Usage:
- Pass to generation via:
  - `--llm-prompt-template-file resources/branding/llm/prompts/<brand_slug>/<market_slug>.txt`

Start from:
- `resources/branding/llm/llm_prompt.brand_market_template_v1.txt`
