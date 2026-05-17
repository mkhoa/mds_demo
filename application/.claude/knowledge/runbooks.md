# Runbooks

Known platform issues and their fixes. Check here before deep investigation.

## Docker socket: "permission denied"

`docker` commands fail with a permission error on `/var/run/docker.sock`.
Cause: the non-root `hermes` user is not in the host's `docker` group. The
Hermes image adds it via the `DOCKER_GID` build arg in
`application/hermes/Dockerfile` (default `987`). If this error appears, the
host GID has likely drifted — run `stat -c %g /var/run/docker.sock` on the
host, then rebuild the Hermes image with `--build-arg DOCKER_GID=<gid>` and
recreate the container. Rebuilding images is out of scope for the operator,
so flag this to a human.

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
