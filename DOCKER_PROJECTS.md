# Multiple QuestDB projects on one host

`docker-compose.yml` runs **one isolated QuestDB instance per project**, each
built from this repo's source, with its own data directory and ports. This is the
recommended way to keep several projects organized without a shared namespace:
the instance boundary itself isolates a project's tables from the others.

## Why per-instance instead of one shared server

QuestDB has no schema/database concept — all tables share a flat namespace, and
there is no per-table access control in the open-source build. Running a separate
instance per project gives real isolation: independent data, restart, upgrade,
and blast radius. The cost is N base memory footprints and N port sets.

## Build and run

```bash
# Build the image from source once (slow: full Maven build incl. web console).
docker compose build

# Start all projects in the background.
docker compose up -d

# Status / logs / stop.
docker compose ps
docker compose logs -f project-a
docker compose down
```

Rebuild after changing repo source so the image picks up new code:

```bash
docker compose build && docker compose up -d
```

## Endpoints

| Project   | Web console / REST | Postgres wire | ILP (line protocol) |
|-----------|--------------------|---------------|---------------------|
| project-a | http://localhost:9000 | localhost:8812 | localhost:9009 |
| project-b | http://localhost:9100 | localhost:8912 | localhost:9109 |

Postgres connection strings (default credentials `admin` / `quest`):

```
postgresql://admin:quest@localhost:8812/qdb   # project-a
postgresql://admin:quest@localhost:8912/qdb   # project-b
```

## Data

Each instance stores everything under a host bind mount:

- `./data/project-a/` -> `/var/lib/questdb` in the container
- `./data/project-b/` -> `/var/lib/questdb`

These hold `db/`, `conf/`, `public/`, etc. `./data/` is git-ignored. Back up a
project by copying its directory while the container is stopped. The container
entrypoint chowns the data dir to its `questdb` user (UID 10001) on start.

## Add another project

Copy a service block in `docker-compose.yml`, then:

1. Rename the service and `container_name` (e.g. `project-c`).
2. Bump every host port by +100 from the previous project: project-c uses
   `9200:9000`, `9012:8812`, `9209:9009`.
3. Point the bind mount at a new dir: `./data/project-c:/var/lib/questdb`.
4. `image: questdb-local:dev` (reuse the built image) with
   `depends_on: [project-a]`.

## Notes

- `ulimits.nofile` is raised to 1048576 because QuestDB memory-maps many files;
  the default container limit is too low for real workloads.
- On Linux hosts under heavy load, raise the host map count:
  `sudo sysctl -w vm.max_map_count=1048576`. Not needed on Docker Desktop / macOS.
- For hard resource caps per project, add `mem_limit` / `cpus` to a service.
- This builds the plain QuestDB image. To run the InfluxDB-v1 + permissions build
  instead, point `build.context`/`dockerfile` at the `qdb-inf` fork's
  `core/Dockerfile` (same ports) once that work lands.
