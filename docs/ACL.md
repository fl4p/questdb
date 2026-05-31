# User access control (acl.conf)

This fork adds a lightweight, file-based user model on top of QuestDB's HTTP
interface: named users with passwords, each either read-write or read-only, and
optionally restricted to tables whose name starts with a given prefix.

It is a fork-level feature. Upstream open-source QuestDB has no multi-user model
(`CREATE USER`/`GRANT` are no-ops) and no per-table authorization.

## Enabling it

Create `conf/acl.conf` in the server root (next to `server.conf`). Its presence
with at least one user turns the feature on; absence or an all-commented file
leaves the server in its default single-user behavior.

```properties
# conf/acl.conf — one block per user.
#   user.<name>.password=<plaintext>
#   user.<name>.access=ro|rw          # default rw
#   user.<name>.prefix=<tablePrefix>  # optional; empty = all tables

user.alice.password=s3cret
user.alice.access=ro
user.alice.prefix=projectA_

user.bob.password=hunter2
user.bob.access=rw
user.bob.prefix=projectB_

user.admin.password=supersecret
# no prefix, rw -> full access
```

Passwords are stored in plaintext, consistent with how QuestDB already stores
`http.user`/`http.password` and the PG-wire passwords in `server.conf`. Keep
`acl.conf` readable only by the server account.

## How it behaves

- **Authentication (HTTP Basic).** Clients send `Authorization: Basic
  base64(user:password)`. This covers the InfluxDB `/query` endpoint, the REST
  API (`/exec`, `/imp`, `/exp`), and the web console. Unknown or wrong
  credentials are rejected.
- **Read-only users** may run queries but no writes or DDL (`Write permission
  denied`).
- **Prefix-scoped users** may only see and touch tables whose name starts with
  their prefix:
  - a `SELECT`/write against an out-of-prefix table is denied;
  - table listings only show in-prefix tables — `tables()`, `SHOW TABLES`,
    `information_schema.tables`, PG-wire `\dt` (`pg_class`), and therefore the
    InfluxDB `SHOW MEASUREMENTS`;
  - `table_columns()` / `SHOW COLUMNS` (and thus InfluxDB `SHOW TAG KEYS` /
    `SHOW FIELD KEYS`) report an out-of-prefix table as non-existent, so its
    schema cannot be enumerated by guessing the name.
- Enforcement is in the engine (the security context), so the same rules apply
  whether a user connects via the InfluxDB API, the REST API, or PG-wire — the
  prefix rule cannot be bypassed by switching protocol.

## Scope and caveats

- **HTTP only for now.** PG-wire keeps its existing `admin`/read-only users;
  these ACL users are validated on the HTTP interface. (PG-wire multi-user auth
  is a possible follow-up.)
- **ILP ingestion (line protocol)** is unchanged; writes still use the existing
  ILP auth, not these users.
- **Web console interactive login** (cookie/session) is not wired for ACL users;
  they authenticate per request via Basic auth, which Grafana and REST clients
  do automatically.
- If both `http.user` (in `server.conf`) and `acl.conf` are set, `acl.conf`
  takes precedence for HTTP.
- A malformed `acl.conf` fails server startup with a clear message rather than
  silently mis-granting access.
