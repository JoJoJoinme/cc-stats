# cc-stats Claude plugin

This plugin ships Claude Code hooks for `cc-stats`.

It expects the local `cc-stats` launcher installed by:

```bash
bash install/client.sh --server-url http://SERVER:8787
```

After that, install the plugin from the local marketplace:

```bash
claude plugin marketplace add /path/to/cc-stats
claude plugin install cc-stats-telemetry@cc-stats --scope project
```
