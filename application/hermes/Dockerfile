FROM nousresearch/hermes-agent:latest

USER root

# Claude Code CLI — the operator delegate Hermes drives for platform tasks.
RUN npm install -g @anthropic-ai/claude-code && claude --version

# curl is not present in the base image (libcurl is, but not the binary).
# Install it so the Docker/Compose download steps below work as written.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Docker CLI + Compose plugin — distro-agnostic static binaries. Used against
# the host Docker socket mounted at /var/run/docker.sock for service lifecycle.
ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.32.4
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" -o /tmp/docker.tgz \
 && tar -xzf /tmp/docker.tgz -C /tmp \
 && mv /tmp/docker/docker /usr/local/bin/docker \
 && rm -rf /tmp/docker /tmp/docker.tgz \
 && mkdir -p /usr/local/lib/docker/cli-plugins \
 && curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" -o /usr/local/lib/docker/cli-plugins/docker-compose \
 && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
 && docker --version && docker compose version

# Put the non-root 'hermes' user in the host's docker group so the operator
# (which runs as 'hermes') can use the mounted Docker socket. DOCKER_GID must
# match `stat -c %g /var/run/docker.sock` on the host.
ARG DOCKER_GID=987
RUN if ! getent group "${DOCKER_GID}" >/dev/null; then groupadd -g "${DOCKER_GID}" dockerhost; fi \
 && usermod -aG "$(getent group "${DOCKER_GID}" | awk -F: '{print $1}')" hermes

COPY claude-ops /usr/local/bin/claude-ops
COPY init.sh /usr/local/bin/hermes-init.sh
RUN mkdir -p /usr/local/share/hermes
COPY SOUL.md /usr/local/share/hermes/SOUL.md
RUN chmod +x /usr/local/bin/claude-ops /usr/local/bin/hermes-init.sh

CMD ["sh", "/usr/local/bin/hermes-init.sh"]
