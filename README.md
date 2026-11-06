# bazaar-contrib-server-management

The default configuration of a Bazaar Compute server. Installed beside `bazaar-compute`, it gives a new host the server's entries: `http`, the server's own database `server-database` and its history `server-migrations`, the server itself `server` with its two themes `server-theme-night` and `server-theme-day`, its overview `server-overview`, the login `server-access`, and the computers `server-computer` with the relay between replicas `server-relay-memory`, each keeping its own settings.

```sh
uv tool install bazaar-compute --with bazaar-contrib-server-management
bcn run
```

A host that also has the node's packages runs both. The settings live in the entries they belong to:

```toml
[[contribs]]
package = "bazaar-contrib-http~=0.3.0rc1"
[[contribs.enabled]]
entry = "http"
[contribs.enabled.config]
listen = "127.0.0.1:8765"

[[contribs]]
package = "bazaar-contrib-server-database~=0.3.0rc1"
[[contribs.enabled]]
entry = "server-database"
[contribs.enabled.config]
storage = "sqlite"
```
