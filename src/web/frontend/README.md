# Web Frontend

React + Vite UI for browsing jobs and details from the web backend.

Current filter surface:
- company chips
- posted-within dropdown
- job-type dropdown
- location dropdowns for `Country`, `Region`, and `City`
- free-text search

Location filter values are loaded from the backend `/get_location_filters` endpoint and are derived from Elasticsearch.

## Environment
- `VITE_API_BASE_URL` (optional, default `http://127.0.0.1:8000`)

## Run Locally
```bash
npm --prefix src/web/frontend install
npm --prefix src/web/frontend run dev
```
UI URL: `http://localhost:5173`

## Build
```bash
npm --prefix src/web/frontend run build
npm --prefix src/web/frontend run preview
```

## Tests
```bash
npm --prefix src/web/frontend run test:coverage
```

Coverage thresholds are enforced in `vite.config.js`.

## Docker
Frontend runs in the unified stack and is served by Nginx:
```bash
make up
```
UI URL: `http://localhost:5173`
