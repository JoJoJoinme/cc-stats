$env:CC_STATS_SERVER_URL = "http://YOUR_SERVER:8787"
& "$env:APPDATA\cc-stats\runtime\Scripts\python.exe" -m cc_stats.cli client watch-costrict --interval 20
