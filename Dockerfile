FROM oven/bun:1.4.3-alpine
WORKDIR /app
# Only Bun's native fetch reaches the upstream with the client fingerprint the
# free-tier gate expects; there are no npm dependencies to install. The exact
# image tag is intentionally pinned so a rebuild cannot silently change the
# transport that every relay depends on.
COPY server.mjs signal-writer.mjs ./
EXPOSE 3000-3064
# No privileged ports or filesystem writes.
USER bun
CMD ["bun", "server.mjs"]
