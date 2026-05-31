# ACL authentication for the PostgreSQL wire protocol

## Context

This fork adds a file-based ACL (`conf/acl.conf`): when present, the server runs
in multi-user mode where each user is prefix-scoped and optionally read-only.
The ACL was enforced on HTTP/REST at the authentication layer
(`MultiUserHttpAuthenticatorFactory`, wired in `FactoryProviderImpl` when
`acl.conf` exists).

The authorization half is already protocol-agnostic and already covers pgwire:
`PGConnectionContext` calls
`securityContextFactory.getInstance(authenticator, SecurityContextFactory.PGWIRE)`,
and `FactoryProviderImpl` already wraps the base factory with
`PrefixAwareSecurityContextFactory` when `acl.conf` is present. So prefix-scoping
and read-only enforcement already apply to whoever authenticates over pgwire.

The missing piece was pgwire authentication: `FactoryProviderImpl` hardcoded
`DefaultPGAuthenticatorFactory`, whose `DynamicUsernamePasswordMatcher` recognizes
only the two config accounts (`pg.user`/`pg.password` and the optional
`pg.readonly.user`) and never consulted the `AclStore`. ACL users could not log in
over pgwire.

This change makes pgwire authenticate against `acl.conf` exactly the way HTTP
does. Decision: imitate HTTP -- when `acl.conf` is present, only ACL users
authenticate over pgwire; the built-in `admin`/readonly accounts are fully
replaced (no silent superuser fallback), mirroring
`MultiUserHttpAuthenticatorFactory`.

## Implementation

1. `AclUsernamePasswordMatcher` (`cairo/security/`) implements
   `UsernamePasswordMatcher`. It looks up the user in `AclStore` and compares the
   stored password against the native UTF-8 wire bytes with
   `Utf8s.equalsUtf16`, using a `DirectUtf8String` flyweight (no owned native
   memory). Unknown/empty username or wrong password returns `AUTH_TYPE_NONE`.

2. `AclPGAuthenticatorFactory` (`cutlass/pgwire/`) mirrors
   `DefaultPGAuthenticatorFactory`, but builds an `AclUsernamePasswordMatcher`
   (one per connection) and plugs it into the unchanged
   `PGCleartextPasswordAuthenticator` (with `matcherOwned = false`).

3. `FactoryProviderImpl` selects the ACL pgwire factory when `acl.conf` is
   present, parallel to the existing HTTP wiring:
   `pgAuthenticatorFactory = aclStore != null ? new AclPGAuthenticatorFactory(...)
   : new DefaultPGAuthenticatorFactory(...)`.

No new server config keys; `acl.conf` presence is the single gate, identical to
HTTP. ILP is out of scope.

## Tests

`PGAclAuthTest` (`test/cutlass/pgwire/`) exercises ACL auth over a real pgwire
socket: valid/wrong/unknown credentials, admin/quest rejection when ACL is
present, prefix filtering of `tables()` and table access, and read-only
enforcement.
