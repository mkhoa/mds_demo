#!/bin/sh
set -e

. /opt/hermes/.venv/bin/activate

# Install the operator persona on every boot. application/hermes/SOUL.md is the
# source of truth, so overwrite whatever default SOUL.md Hermes scaffolds in
# /opt/data. To change the persona, edit application/hermes/SOUL.md and rebuild.
if [ -f /usr/local/share/hermes/SOUL.md ]; then
  cp /usr/local/share/hermes/SOUL.md /opt/data/SOUL.md
fi

# Point Hermes' own model at LiteLLM (mirrors the llm-webui-app init.sh).
if [ -f /opt/data/config.yaml ]; then
  sed -i \
    -e "s|^  default: \".*\"\$|  default: \"$HERMES_MODEL\"|" \
    -e "s|^  provider: \".*\"\$|  provider: \"$HERMES_PROVIDER\"|" \
    -e "s|^  base_url: \".*\"\$|  base_url: \"$HERMES_BASE_URL\"|" \
    /opt/data/config.yaml
  if grep -q '^  api_key: ' /opt/data/config.yaml; then
    sed -i "s|^  api_key: \".*\"\$|  api_key: \"$HERMES_API_KEY\"|" /opt/data/config.yaml
  else
    sed -i "s|^  # api_key: \"your-key-here\".*\$|  api_key: \"$HERMES_API_KEY\"|" /opt/data/config.yaml
  fi
fi

exec hermes gateway run
