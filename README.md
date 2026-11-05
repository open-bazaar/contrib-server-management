# bazaar-contrib-server-management

Run a Bazaar Compute host as the server. The package registers the server's default configuration under `bazaar_compute_node.defaults` and assembles the server composition with private bindings, on a sqlite database of its own at `~/.bcn/bcs.sqlite3`. On the first start it brings a 0.2 server's settings, signing key and database over from `~/.bcs`, which stays as it was.

```sh
uv tool install bazaar-compute --with bazaar-contrib-server-management
bcn run
```

A new host installed with this package runs as the server; installed beside `bazaar-compute-node`, it runs both. The `server-management` entry in `config.toml` keeps 0.2's settings and their defaults:

```toml
[[contribs]]
package = "bazaar-contrib-server-management~=0.3.0rc1"
[[contribs.enabled]]
entry = "server-management"
[contribs.enabled.config]
listen = "127.0.0.1:8765"
session_minutes = 10
retention_days = 30
storage = "sqlite"
```
