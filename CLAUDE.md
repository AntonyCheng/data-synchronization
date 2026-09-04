# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

A single Git repo containing one product: a **data synchronization platform** that wraps
**Apache SeaTunnel 2.3.13 (Zeta engine)** as a configurable, observable, recoverable sync
product. Source is **MySQL** (binlog CDC); MVP targets are **PostgreSQL, MySQL and Kafka**.

The platform does **not** implement CDC itself and does **not** store or assemble engine
checkpoints — SeaTunnel checkpoint/savepoint is the authoritative recovery source. The
platform owns configuration, precheck/type-mapping, state aggregation, reconciliation,
scheduling, and the SeaTunnel REST adapter.

## Repository layout

| Path | Contents |
|---|---|
| `server/` | RuoYi-Vue-Plus 6.0.0 backend (Spring Boot 4.1, Java 21, Maven multi-module). Upstream scaffold — **only `ruoyi-modules/ruoyi-sync` is this project's code.** |
| `web/` | plus-ui-react 6.0.0 frontend (React 19, Umi Max, Ant Design 6 + ProComponents, pnpm). |
| `platform/` | Docker Compose for dev-stage metadata MySQL (`dbs-mysql`, host `13306`, db `ry-vue`) and Redis (`dbs-redis`, host `16379`). |
| `test/` | Fully isolated SeaTunnel POC environment (Compose project `data-sync-poc`, `ds-poc-*` containers) plus PowerShell POC/acceptance scripts. Never touches `platform/`. |
| `docs/` | The canonical documentation tree, staged `01-product` … `06-release`. Start at `docs/README.md`. |

All product API routes live under `/sync`; backend package is `org.dromara.sync`.

## Essential commands

### One command for the whole local stack — `dev.ps1` (repo root)

`dev.ps1` orchestrates docker (base services) + java21 (backend) + pnpm (frontend).
Windows PowerShell 5.1 compatible (there is no `pwsh` on the dev machine).

```powershell
.\dev.ps1 up                 # containers(health-gated) -> migrations(idempotent) -> backend ∥ frontend -> prints URLs
.\dev.ps1 up -Fast           # + dev-fast profile: disables workflow/LiteFlow + lazy-init, ~15s faster boot
.\dev.ps1 up -Poc            # + start the SeaTunnel POC stack (ds-poc-*)
.\dev.ps1 up -NoBuild        # skip the reactor build, go straight to spring-boot:run (when nothing changed)
.\dev.ps1 down [-All]        # stop backend+frontend; -All also `docker compose stop`
.\dev.ps1 status             # containers / ports / PIDs
.\dev.ps1 restart-backend    # reinstall ruoyi-sync + ruoyi-admin (no fat jar) then relaunch (~1.5 min)
.\dev.ps1 logs backend|frontend
```

`up` runs the backend as: reactor `install` with the `spring-boot:repackage` fat-jar
step skipped (~35 s warm, vs ~4 min for the old `package` path), then `mvn spring-boot:run`
from `server/ruoyi-admin/`. Login: `admin` / `admin123` (captcha on).

### Backend — manual / release path

Java 21 + local `mvn` (or `./mvnw`). Default profile `dev` (`application-dev.yml`);
backend on **:18081**.

```powershell
server\script\bin\run-backend-dev.ps1 [-Fast] [-Background] [-SkipBuild]  # reactor install (no fat jar) + spring-boot:run — what dev.ps1 uses
server\script\bin\start-backend-dev.ps1 [-Background]         # fat-jar path — release / offline-package verification
mvn -pl ruoyi-admin -am package -DskipTests                   # build the full ruoyi-admin.jar (maven.test.skip=true in root pom)
```

`run-backend-dev.ps1` runs `spring-boot:run` from the `ruoyi-admin/` module dir (the
`spring-boot` prefix and plugin version only resolve correctly there, not from the
aggregator). Business-module deps load from the local repo, so a change outside
`ruoyi-admin` needs a reinstall — that's what `dev.ps1 restart-backend` does.

`-Fast` / the `dev-fast` profile (`application-dev-fast.yml`) turns off warm-flow +
LiteFlow and enables `spring.main.lazy-initialization`; `@Scheduled` beans stay eager
via `org.dromara.config.DevFastLazyInitConfig`. Workflow pages 500 under `-Fast`.
Dev-profile default also disables Snail-AI OpenAPI and MyBatis SQL logging (override
with `SNAIL_AI_OPENAPI_ENABLED` / `MYBATIS_SQL_LOG`).

Tests are **skipped by default**. Surefire filters by JUnit `@Tag` equal to the active
profile, so runnable tests are tagged `@Tag("dev")`. To run the `ruoyi-sync` regression:

```powershell
mvn -Pdev -pl ruoyi-modules/ruoyi-sync -am -Dmaven.test.skip=false -Dgroups=dev -Dsurefire.failIfNoSpecifiedTests=false test
# single class(es): append -Dtest=SyncTaskSchedulerTest,ResourceProtectionPolicyTest
```

### Frontend — manual (run from `web/`)

pnpm 10 / Node ≥ 20.19. Dev server port comes from `web/.env.development`
`VITE_APP_PORT` (currently **8003**); proxies `/dev-api` + `/prod-api` → `:18081`.

```powershell
pnpm install
pnpm dev            # Umi/Vite dev server; MFSU is disabled (config.ts), Vite optimizeDeps handles pre-bundle
pnpm lint           # oxlint src config vite.config.ts, then `max setup` + tsc --noEmit
pnpm run fmt        # oxfmt
pnpm build          # runs `max setup` then production build
```

`tsc` needs generated Umi types, so `lint`/`build` run `pnpm run setup:umi` (`max setup`)
first; run it manually before a bare `pnpm exec tsc --noEmit`. Do **not** delete
`web/node_modules/.vite` — that cache is what keeps `pnpm dev` startup fast.

### Local infrastructure & metadata schema (done for you by `dev.ps1 up`)

```powershell
docker compose --project-name data-sync-platform --file platform/docker-compose.yml up -d   # dbs-mysql :13306 / dbs-redis :16379
# ry_sync.sql is the first-init seed; ry_sync_migration_*.sql apply in filename order, idempotent:
test\scripts\migrate-platform-schema.ps1
```

### SeaTunnel POC (isolated, from repo root)

```powershell
test\scripts\up.ps1              # build images + start ds-poc-* containers
test\scripts\run-poc.ps1         # baseline MySQL -> PostgreSQL full + CDC correctness
test\scripts\down.ps1            # stop, keep all data
test\scripts\reset-poc.ps1 -Force  # wipe test/runtime/ (keeps test/results/)
```

Many scenario scripts exist (`run-mysql-target-*.ps1`, `run-mysql-kafka-*.ps1`,
`recovery-drill.ps1`, `phase4/5/6-acceptance.ps1`, …); see `test/README.md`. Run
evidence goes to `test/results/<timestamp>/`; conclusions are written back into
`docs/03-poc/`.

## Architecture of the sync module (`server/ruoyi-modules/ruoyi-sync`)

### Domain objects (tables prefixed `ds_`, defined in `server/script/sql/ry_sync.sql`)

- **`DataSource`** — connection info + AES-encrypted credential. `MYSQL` only as source; `POSTGRESQL`/`MYSQL`/`KAFKA` as target.
- **`SyncTask`** — one single-table task; carries `engine_job_id`, schedule state, `config_version`, `last_error`, and (for Kafka) `kafka_*` publish metrics.
- **`SyncTaskGroup` + `SyncTaskGroupItem`** — multi-table / whole-database (`syncScope` = `MULTI_TABLE` | `DATABASE`), ≤ 20 tables, **one SeaTunnel job per table item**.
- **`SyncTaskConfigVersion`** — immutable, credential-free config snapshot per create/edit, keyed `(task_id, config_version)`. A checkpoint is bound to the version that produced it.

Table/column metadata is read on demand from source JDBC and **not persisted**.

### Key services (`service/impl/`)

- `SeaTunnelJobServiceImpl` / `SyncTaskGroupServiceImpl` — lifecycle: preview → validate → start/status/pause/resume/stop, via `SeaTunnelRestClient`. Pause = `stop-job` with `isStopWithSavePoint=true`; resume = `submit-job` with `isStartWithSavePoint=true` reusing the original `jobId`.
- `SeaTunnelJobConfigGenerator` / `SyncTaskGroupConfigGenerator` — build the HOCON job config. The API-facing copy always has `password` replaced with `******`; the platform persists only a **SHA-256 fingerprint** of the real config, never its body. `server-id` is derived deterministically from task id.
- `DataSourceMetadataServiceImpl` — JDBC introspection: databases/tables/columns/keys/charset, CDC precheck, target compatibility.
- `DataConsistencyServiceImpl` — read-only reconciliation: `COUNT(*)` mode, or `KEY_RANGE` block comparison for a single numeric sync key. Persists last result into `last_check_*` columns.
- `SyncTaskScheduler` — `ONCE` / `CRON` (Spring 6-field) triggering; shares the start service and the per-task Redis lock with manual start.
- `KafkaTaskBridgeService` + `KafkaEventNormalizer` + `KafkaEventProducer` — for Kafka targets: SeaTunnel writes a versioned raw Debezium topic, the bridge normalizes it into the PRD standard event envelope on a stable partition key (`acks=all`, idempotent).

### Invariants (see `docs/02-architecture/system-architecture.md`, `docs/04-design/task-state-machine.md`)

- Platform state must be reconciled against engine state — on `ApplicationReadyEvent` the module scans `RUNNING`/`PAUSING` tasks and refreshes status; unreachable jobs become `FAILED` (never silently "running").
- No concurrent runs of one logical task — Redis lock `sync:task:start:<id>` (Redisson).
- Relational targets do idempotent upsert + delete on the sync key; the sync key must match the source PK or an all-`NOT NULL` unique index.
- A semantic config-version change forbids reusing the old checkpoint; `REINITIALIZE_REQUIRED` cannot be bypassed by plain start/resume.
- State set: `DRAFT`, `RUNNING`, `PAUSING`, `PAUSED`, `STOPPED`, `FAILED`, `REINITIALIZE_REQUIRED`, `FINISHED`. Only `DRAFT`/`STOPPED`/`REINITIALIZE_REQUIRED` are editable and startable from config.
- `sync.engine.connection-endpoint-overrides` (in `application-dev.yml`) rewrites host ports (e.g. `localhost:23306`) to compose-network endpoints (`ds-poc-mysql:3306`) because SeaTunnel runs inside Docker while the backend runs on the host.

`docs/04-design/api-contract.md` is the authoritative endpoint + request/response reference.

## Coding conventions

This is a RuoYi-Vue-Plus project; both `server/` and `web/` carry agent/skill rule
files that encode the house style. **Read the relevant one before writing code:**

- Backend: `server/.claude/agents/backend-engineering.md` (router) →
  `server/.codex/skills/ruoyi-plus-ai-coding/SKILL.md` + `references/backend.md`.
- Frontend: `web/.claude/agents/frontend-crud-coding.md` (router) →
  `web/.codex/skills/frontend-crud-coding/SKILL.md` + `references/frontend.md`.

Load order in both: nearest existing code in the same module wins, then shared
`ruoyi-common` / shared hooks, then the generator templates, then generic
Spring/MyBatis-Plus or React/Antd habits.

### Backend

- Layering: `domain` / `domain.bo` / `domain.vo` / `mapper` / `service` / `service.impl` / `controller`. Never put business logic in the controller.
- Use the framework types as-is: `BaseMapperPlus<Entity,Vo>`, `PageQuery` / `PageResult<T>`, `R<T>`, `MapstructUtils.convert`, `@AutoMapper`, `StringUtils`/`StreamUtils`. Do not substitute homegrown equivalents.
- Controllers extend `BaseController`; annotate with `@SaCheckPermission("sync:<business>:<action>")`, `@Log(businessType=...)`, and `@RepeatSubmit` where neighbours do.
- BO holds request/query fields; VO holds derived/display fields; VO never carries `password`.
- `.editorconfig`: UTF-8, LF, 4-space indent (2 for `json`/`yml`/`yaml`), final newline (not for `.md`). Don't reorder imports or reformat untouched code.

### Frontend

- React 19 + Umi Max + Ant Design 6 + ProComponents + ahooks + TanStack Query + Zustand. Pages in `src/pages/<module>/<business>/index.tsx`; API in `src/api/<module>/<business>/{index.ts,types.ts}`.
- Requests go through `@/api/request`; outer type `R<T>` / `PageResult<T>` from `@/api/types`. Never import `AxiosPromise` or Vue / Element Plus / `src/views` patterns.
- Reuse the project hooks/utils: `ProTable`, `ModalForm`, `RowActions`, `useTableSelection`, `useTableExport`, `useDateRangeQuery`, `dictOptions`, `useTreeTableExpand`, `useLoading`, `useSearchReset`, `confirmAction`/`confirmTitleSafe`, `toPageQuery`, `toTableData`, `handleTree`.
- Permissions: `useUserStore(state => state.userInfo)` + `hasPermi(userInfo, ['sync:business:action'])`.
- 2-space indent; format with `pnpm run fmt`, lint with `pnpm lint`.

## Credentials & environment gotchas

- Credential encryption is gated by env vars, decrypted only in the service layer:
  `SYNC_CREDENTIAL_ENCRYPTION_ENABLED=true` + `SYNC_CREDENTIAL_ENCRYPTION_PASSWORD=<16|24|32 chars>`
  (`mybatis-encryptor` in yaml). Plaintext POC rows stay readable; a save (blank password
  field keeps the current value) migrates a row to ciphertext. See `docs/04-design/credential-protection.md`.
- The backend start scripts set `-Djdk.net.unixdomain.tmpdir` to an ASCII path under
  `test/runtime/jdk-sockets` — a JDK 21 / Redisson issue when the Windows user profile
  path contains non-ASCII characters. Keep that workaround if you touch startup.
  Shared PowerShell helpers live in `server/script/bin/common.ps1` (dot-sourced by
  `dev.ps1`, `run-backend-dev.ps1`, `start-backend-dev.ps1`).
- Never commit `.env` files, RSA keys, `server/logs/`, `**/target/`, `node_modules/`,
  Docker volumes (`platform/runtime/`, `test/runtime/`), or `test/results/` contents.
  Commit only `.env.example` templates. See `docs/06-development/git-strategy.md`.
- Commit message style in history is Conventional-Commits-ish with a scope for repo-wide
  changes: `poc:`, `release:`, `docs(repo):`, `chore(repo):`.

## Codex config import

`server/.codex/` and `web/.codex/` hold Codex skills (`ruoyi-plus-ai-coding`,
`frontend-crud-coding`) that mirror the `.claude/agents` rules above. If you want the
user-level items imported into Claude Code, reply `/import` to see what's importable.
