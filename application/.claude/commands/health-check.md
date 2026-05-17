---
description: Sweep every platform service and report its health
---

Use the `operating-services` skill. Run a full health-check sweep of the
MDS Demo platform:

1. `docker compose -p mds_demo -f /workspace/docker-compose.yml ps`
2. For every service, report: state (running / restarting / exited) and,
   where a healthcheck exists, its health status.
3. For anything not running or not healthy, pull `logs --tail 50` and
   summarize the likely cause.

Finish with a one-line overall verdict: HEALTHY or NEEDS ATTENTION.
