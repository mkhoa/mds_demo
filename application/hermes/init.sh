#!/bin/sh
set -e

# Make the mounted Docker socket usable for service lifecycle. Best-effort:
# only succeeds when this entrypoint runs as root (the image default).
chmod 666 /var/run/docker.sock 2>/dev/null || true

. /opt/hermes/.venv/bin/activate

# Install the operator persona on first boot (does not overwrite edits).
if [ -f /usr/local/share/hermes/SOUL.md ] && [ ! -s /opt/data/SOUL.md ]; then
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
