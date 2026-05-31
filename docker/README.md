# QuestDB container for e2e testing (Podman)

A disposable, single QuestDB instance for end-to-end tests. It uses the
**official prebuilt image** (not this repo's source), stores data in **tmpfs**
so every run starts clean, and exposes a **healthcheck** so a test harness can
wait for readiness before sending traffic.

This is separate from the repo-root `docker-compose.yml`, which builds QuestDB
from source and runs one persistent instance per project. Use that one for
development; use this one for throwaway test runs.

## macOS prerequisite

Podman runs containers inside a Linux VM on macOS. Start it once per boot:

```bash
podman machine init    # first time only
podman machine start
```

## Quickest path: `podman run`

This needs no compose backend, so it behaves identically everywhere - the most
reliable option for CI:

```bash
podman run -d --name questdb-e2e \
  -p 9000:9000 -p 8812:8812 -p 9009:9009 -p 9003:9003 \
  --tmpfs /var/lib/questdb \
  -e QDB_TELEMETRY_ENABLED=false \
  docker.io/questdb/questdb:latest

# Wait until ready (min server on 9003 accepts connections once the DB is up):
until podman exec questdb-e2e bash -c '</dev/tcp/127.0.0.1/9003' 2>/dev/null; do sleep 0.5; done

# ... run tests against http://localhost:9000 / pg 8812 / ilp 9009 ...

podman rm -f questdb-e2e     # tmpfs data is discarded with the container
```

## Compose path

Podman reads the same `docker-compose.yml`:

```bash
# Whichever you have installed:
podman compose -f docker/docker-compose.yml up -d
# or
podman-compose -f docker/docker-compose.yml up -d

# podman-compose has no `--wait`, so gate on the health port yourself:
until curl -sf http://localhost:9003 >/dev/null; do sleep 0.5; done

# Tear down (tmpfs data is discarded automatically).
podman compose -f docker/docker-compose.yml down
```

A clean slate between tests is just `down` + `up` again - no volume to wipe.

`podman compose` delegates to whatever provider is installed (`podman-compose`
or Docker's compose plugin). If you have neither, `podman run` above is the
fallback. Install the Python provider with `pip install podman-compose` if you
prefer compose.

## Endpoints

| Purpose                | URL / address          |
|------------------------|------------------------|
| Web console / REST     | http://localhost:9000  |
| Postgres wire          | localhost:8812         |
| ILP (line protocol)    | localhost:9009         |
| Health (`/status`)     | http://localhost:9003/status |

Postgres connection string (default credentials `admin` / `quest`):

```
postgresql://admin:quest@localhost:8812/qdb
```

## Configuration via env

All ports and the image version are overridable in the compose file, which is
handy for a CI matrix or for running alongside the repo-root `project-a` (which
also uses 9000/8812/9009):

```bash
QDB_VERSION=8.2.3 \
QDB_HTTP_PORT=9500 QDB_PG_PORT=8512 QDB_ILP_PORT=9509 QDB_HEALTH_PORT=9503 \
  podman compose -f docker/docker-compose.yml up -d
```

- `QDB_VERSION` defaults to `latest`. **Pin it for reproducible CI** - `latest`
  is convenient locally but makes test runs non-deterministic over time.
- Any QuestDB setting can be passed as an env var with the `QDB_` prefix and
  dotted keys uppercased with underscores (e.g. `cairo.max.uncommitted.rows`
  becomes `QDB_CAIRO_MAX_UNCOMMITTED_ROWS`). Add them under `environment:` in
  the compose file, or as `-e` flags on `podman run`.

## Podman-specific notes

- **Rootless** is the common Podman default and works fine here: tmpfs lives in
  the user namespace and the image's `questdb` user (UID 10001) maps cleanly.
- **`ulimits.nofile`** in the compose file requests 1048576. Under rootless
  Podman the *hard* limit is capped by your login limits; if startup complains,
  either raise the user limit or lower the value. The `podman run` line omits it
  and relies on Podman's defaults, usually adequate for e2e.
- **SELinux hosts** (Fedora/RHEL): a bind mount (see below) needs a relabel
  suffix, e.g. `-v ./data/e2e:/var/lib/questdb:Z` on `podman run`, or `:Z` on
  the compose `volumes:` entry. tmpfs needs no relabeling.
- **Fully-qualified image name** (`docker.io/questdb/questdb`) is used so Podman
  doesn't prompt to pick a registry for the short name.

## Gating another service on readiness (compose)

```yaml
services:
  tests:
    image: my-e2e-suite
    depends_on:
      questdb:
        condition: service_healthy
```

The healthcheck probes the min server on port 9003 via bash `/dev/tcp`, so it
needs no `curl`/`wget` inside the image. `depends_on.condition` is honored by
`podman compose` with a recent provider; `podman-compose` support varies, so the
explicit poll loops above are the portable fallback.

## Inspecting on-disk data

tmpfs is wiped on container removal and lives in RAM. If a test needs to inspect
the actual files QuestDB writes, or to survive a container restart, replace the
`tmpfs:` block in `docker-compose.yml` with a bind mount (add `:Z` on SELinux):

```yaml
    volumes:
      - ./data/e2e:/var/lib/questdb
    # (remove the tmpfs: block)
```

For `podman run`, swap `--tmpfs /var/lib/questdb` for
`-v ./data/e2e:/var/lib/questdb` (append `:Z` on SELinux). `./data/` is already
git-ignored by the repo root.

## Notes

- `restart: "no"` is deliberate: an e2e container should exit when the run ends,
  not resurrect itself.
- On Linux hosts under heavy load: `sudo sysctl -w vm.max_map_count=1048576`.
