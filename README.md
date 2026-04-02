# ScorePostgres

A PostgreSQL backend for ScoreData, providing durable key-value persistence with full ACID transaction support.

## Usage

```swift
import Score
import ScorePostgres

let config = PostgresConfig(
    host: "localhost",
    port: 5432,
    username: "score",
    password: "secret",
    database: "myapp"
)

let backend = try await PostgresBackend(config: config)
let store = KVStore(backend: backend)

// Same API as any other KVStore
try await store.set(["users", "123"], value: user)
let user: User? = try await store.get(["users", "123"])
```

## Table Schema

The backend creates a single table on first connection:

```sql
CREATE TABLE IF NOT EXISTS score_kv (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL,
    versionstamp BIGINT NOT NULL
);
```

## When to Use

| Backend | Use Case |
|---------|----------|
| `.memory()` | Sessions, caches, tests |
| `.persistent()` (SQLite) | Single-server apps, development |
| **PostgresBackend** | Multi-server, managed database, production at scale |
