# Scripts

## Proxy API CLI

Use the CLI to call internal `proxy-api` endpoints.

```bash
python3 scrapers/proxy/scripts/proxy_api_cli.py --help
python3 scrapers/proxy/scripts/proxy_api_cli.py <command> --help
```

Global options:
- `--api-url` (default: `http://localhost:8090`, or `JOBSEARCH_PROXY_API_URL`)
- `--timeout-seconds` (default: `10`, or `JOBSEARCH_PROXY_API_TIMEOUT_SECONDS`)

Commands:
- `health` -> `GET /health`
- `sizes --scope <scope>` -> `GET /sizes`
- `lease --scope <scope>` -> `POST /lease`
- `release --resource <resource> --token <token> --scope <scope>` -> `POST /release`
- `block --resource <resource> --token <token> --scope <scope>` -> `POST /block`
- `try-enqueue --resource <resource> --capacity <capacity> --scope <scope>` -> `POST /try-enqueue`
- `state --resource <resource> --scope <scope>` -> `GET /state?resource=...`

Examples:

```bash
python3 scrapers/proxy/scripts/proxy_api_cli.py health
python3 scrapers/proxy/scripts/proxy_api_cli.py sizes --scope jobs.apple.com
python3 scrapers/proxy/scripts/proxy_api_cli.py lease --scope jobs.apple.com
python3 scrapers/proxy/scripts/proxy_api_cli.py try-enqueue --resource http://1.2.3.4:8080 --capacity 128 --scope jobs.apple.com
python3 scrapers/proxy/scripts/proxy_api_cli.py state --resource http://1.2.3.4:8080 --scope jobs.apple.com
```

## Proxy Dev Helpers

Start local redis + producer and tail logs:

```bash
./scrapers/proxy/scripts/proxy_dev_up.sh
```

Stop and remove local redis + producer containers:

```bash
./scrapers/proxy/scripts/proxy_dev_down.sh
```

Check Redis proxy lease structures directly:

```bash
python3 scrapers/proxy/scripts/proxy_health_check.py
```
