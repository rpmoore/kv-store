# AGENTS.md — kv-store

## Architecture Overview

Two-service Rust workspace: a **kvstore** (HTTP frontend) and a **storage** node (gRPC backend), sharing types via the **common** library.

```
Client → kvstore (actix-web :8080) → storage (tonic gRPC :50051)
                ↓                           ↓
         SQLite (data.db)            RocksDB partitions
         tenants / namespaces        key-value data
```

- `common/` — shared proto-generated types (`common::storage`, `common::admin`), JWT auth traits, healthcheck, and CRC64 hasher. Proto files live in `common/proto/`; they are compiled by `common/build.rs` using `tonic_prost_build`.
- `kvstore/` — HTTP API (actix-web). Looks up tenants and namespaces in SQLite, then proxies key operations to `storage` over gRPC. Auth tokens are issued and validated here via RSA JWTs.
- `storage/` — gRPC server (tonic). Stores data in RocksDB partitions. A `PartitionLookup` maps `(tenant_id, namespace_id)` → `[Partition]` using consistent jump hashing (`jumphash`). Partition state is persisted to `namespaces/partitions.json`.

## Key Data Flows

**PUT** `PUT /namespaces/{ns}/keys/{id}` (kvstore) → validates JWT → resolves namespace UUID from SQLite → computes CRC32 of `key||value` → forwards `PutRequest` gRPC → storage validates CRC → locates partition via jump hash → writes atomically to RocksDB (data in `default` CF, metadata with CRC+version in `metadata` CF).

**Auth propagation**: `kvstore` extracts the `Authorization: Bearer <token>` header (`common::auth::AuthHeader`) and forwards it as-is in the tonic `MetadataMap` to `storage`. The storage `AuthInterceptor` validates the JWT and injects `Identity` into request extensions; handlers retrieve it via `request.extensions().get::<Identity>()`.

## Developer Workflows

```bash
# Build
just build          # cargo build
just release        # cargo build --release

# Generate RSA keys required at runtime (both services read key.pem / key.pub from CWD)
just init-ssl       # openssl genrsa -out key.pem 2048 && openssl rsa -in key.pub ...

# Dev tooling
just dev-install    # installs cargo-audit and cargo-watch
cargo audit         # check for CVEs
cargo watch         # rebuild on file changes

# Run services (each from their own directory or with -p)
cargo run -p storage   # starts gRPC server on [::1]:50051; reads key.pub from CWD
cargo run -p kvstore   # starts HTTP on 0.0.0.0:8080; healthcheck on :8081; reads key.pem + key.pub from CWD
```

Both services auto-create their databases/directories on first start (`data.db` for kvstore, `namespaces/` dir for storage).

## Project-Specific Conventions

- **Proto-first types**: All cross-service types come from `common/proto/*.proto`. When adding new RPC fields, edit the `.proto` then rebuild `common` — do not manually add fields to the generated modules.
- **Logging**: Debug builds use human-readable `tracing_subscriber::fmt`; release builds use `json()` format. Use `tracing`/`tracing_attributes::instrument` for all instrumentation. Auth tokens are **never** logged — `Token::Display` emits a SHA-384 hash instead of the raw value.
- **CRC integrity**: CRC32 (`crc32fast`) is computed over `key || value` bytes in both kvstore and storage independently; mismatches return `InvalidArgument`. Always include `crc` in `PutRequest`.
- **Tenant isolation**: All queries to SQLite and all RocksDB partition lookups are scoped by `tenant_id` (UUID from JWT `sub` claim). Namespace names are unique per tenant.
- **gRPC client cloning**: `StorageClient` is always `.clone()`d before calling a method because tonic requires `&mut self`; this is cheap per tonic's design (see comments in `kvstore/src/main.rs`).
- **Partition routing**: Keys route to partitions via `CustomJumpHasher<Crc64Hasher>` (consistent hashing). Adding partitions calls `PartitionLookup::add_partition`, which persists state to `namespaces/partitions.json`.
- **Unimplemented stubs**: Several RPC methods (`create_namespace`, `delete_namespace`, `delete`, `migrate_to_new_node`, `get_metadata`) are `todo!()`. Do not rely on them.
- **JWT validation**: Token expiry (`validate_exp`) is disabled in `RsaJwtValidator` — marked as a TODO for production.

## Key Files

| File | Purpose |
|------|---------|
| `common/proto/storage.proto` | Service contract between kvstore and storage |
| `common/src/auth.rs` | JWT issuance/validation, `AuthHeader` actix↔tonic conversion |
| `storage/src/partition.rs` | RocksDB partition: `put`/`get`/`list_keys`/`delete` |
| `storage/src/lookup.rs` | Jump-hash partition router; persists to `partitions.json` |
| `kvstore/src/main.rs` | HTTP routes, SQLite schema creation, service wiring |
| `kvstore/src/namespace.rs` | Namespace SQLite repository |
| `common/src/healthcheck.rs` | Shared healthcheck server (separate port) |

