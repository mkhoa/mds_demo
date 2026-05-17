---
description: Restart a single platform service
argument-hint: <service name>
---

Use the `operating-services` skill. Restart the service: `$ARGUMENTS`

1. Show its current state with `ps`.
2. `docker compose -p mds_demo -f /workspace/docker-compose.yml restart $ARGUMENTS`
3. Wait, then confirm it is running again and show fresh `logs --tail 30`.

Never run `up`/`down`/`build` — only `restart`.
