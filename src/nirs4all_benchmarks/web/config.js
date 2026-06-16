// Runtime config. The static-site build (site_build.py) overwrites this file with
// `window.ARENA_STATIC = true;` so the SPA queries the bundled JSON snapshot instead
// of a live /api backend. In dev (served by FastAPI) it stays false.
window.ARENA_STATIC = false;
