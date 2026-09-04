# Data Synchronization Platform

MySQL binlog CDC synchronization platform. MVP targets: PostgreSQL, MySQL and Kafka.

## Project Layout

- `server/`: RuoYi-Vue-Plus 6.0.0 backend. Only `ruoyi-modules/ruoyi-sync` is this project's code.
- `web/`: plus-ui-react 6.0.0 frontend.
- `platform/`: development metadata MySQL and Redis Compose services.
- `deploy/`: local SeaTunnel engine stack, metadata-schema migration script, offline delivery template.
- `docs/`: architecture and design reference.

Start with [docs/README.md](docs/README.md), then the repo-root `CLAUDE.md` for how to run
the stack (`dev.ps1`).
