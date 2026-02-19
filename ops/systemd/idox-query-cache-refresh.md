# idox-query-cache-refresh

Runs all `queries/cache_*.sql` once per day and updates `public.query_cache`.
`queries/import_time.sql` is intentionally excluded.

## Install

```bash
sudo cp /opt/scraper/ops/systemd/idox-query-cache-refresh.service /etc/systemd/system/
sudo cp /opt/scraper/ops/systemd/idox-query-cache-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now idox-query-cache-refresh.timer
```

## Verify

```bash
systemctl status idox-query-cache-refresh.timer
systemctl list-timers --all | grep idox-query-cache-refresh
journalctl -u idox-query-cache-refresh.service -n 200 --no-pager
```

## Manual run

```bash
sudo systemctl start idox-query-cache-refresh.service
```
