# ai-to-mapper

## Overview
- Backend: Bun REST API with plugin-based route registration
- Frontend: React (Vite) served by Bun

## Requirements
- Bun installed

## Install
From the repo root:

```bash
bun install
```

## Run (dev)
From the repo root:

```bash
bun run dev
```

- Frontend: http://localhost:5173
- Backend: http://localhost:3001
- Health endpoint: http://localhost:3001/api/health

## Backend plugin architecture
- Plugins live in `apps/backend/src/plugins/*.plugin.ts`
- Each plugin exports a default `Plugin` with a `register(ctx)` function
- Plugins register routes via `ctx.registerRoute({ method, path, handler })`
