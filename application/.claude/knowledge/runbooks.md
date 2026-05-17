# Runbooks

Known platform issues and their fixes. Check here before deep investigation.

## Docker socket: "permission denied"

`docker` commands fail with a permission error on `/var/run/docker.sock`.
Cause: the socket's group is not accessible to the operator user. The Hermes
`init.sh` runs `chmod 666` on the socket at startup; if the container was
started before that ran, restart `hermes`. If it persists, the host socket
group GID must be added to the container — flag this to a human.

## A service shows "unhealthy"

Check the healthcheck target. `minio`, `warehouse_db`, and `redis` have
healthchecks; an unhealthy state usually means the service is still starting
(wait) or its dependency is down (check `warehouse_db` first — most services
depend on it).

## dbt run fails: "relation does not exist"

An upstream model or source is not built. Build the upstream layer first
(`raw`/`stg`/`bdh`) or run the Mage pipeline that loads the source, then retry.

## Mage pipeline not found

Pipeline names come from directories under
`application/mage_ai/mds_demo/pipelines/`. List that folder to get exact names.
