import Foundation
import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import ScoreData

/// A PostgreSQL-backed KV backend for ScoreData.
///
/// Stores key-value pairs in a PostgreSQL table, providing durable
/// persistence with full ACID transaction support. Suitable for
/// production deployments where data must survive process and
/// machine restarts.
///
/// ### Setup
///
/// ```swift
/// let backend = try await PostgresKVBackend(config: PostgresKVConfig(
///     host: "localhost",
///     username: "score",
///     password: "secret",
///     database: "myapp"
/// ))
/// let store = KVStore(backend: backend)
/// ```
///
/// The backend creates the following table on first connection:
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS score_kv (
///     key TEXT PRIMARY KEY,
///     value BYTEA NOT NULL,
///     versionstamp BIGINT NOT NULL
/// )
/// ```
public final class PostgresKVBackend: KVBackend, @unchecked Sendable {

    private let client: PostgresClient
    private let tableName: String
    private let lock = NSLock()
    private var nextVersion: UInt64

    /// Creates a backend connected to the given PostgreSQL server.
    ///
    /// The client must have its `run()` method active in a background task
    /// for queries to work. The table is created automatically if it does
    /// not exist.
    ///
    /// - Parameters:
    ///   - client: A running `PostgresClient` instance.
    ///   - tableName: The table name for KV storage. Defaults to `"score_kv"`.
    /// - Throws: If table creation or version query fails.
    public init(client: PostgresClient, tableName: String = "score_kv") async throws {
        self.client = client
        self.tableName = tableName

        // Create table
        try await client.query(
            PostgresQuery(
                unsafeSQL: """
                    CREATE TABLE IF NOT EXISTS \(tableName) (
                        key TEXT PRIMARY KEY,
                        value BYTEA NOT NULL,
                        versionstamp BIGINT NOT NULL
                    )
                    """))

        // Read max versionstamp
        let rows = try await client.query(
            PostgresQuery(unsafeSQL: "SELECT COALESCE(MAX(versionstamp), 0) FROM \(tableName)"))
        var maxVersion: Int64 = 0
        for try await (v, ) in rows.decode(Int64.self) {
            maxVersion = v
        }
        self.nextVersion = UInt64(maxVersion) + 1
    }

    // MARK: - Helpers

    private func bumpVersion() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        let v = nextVersion
        nextVersion += 1
        return v
    }

    private func keyString(_ key: KVKey) -> String {
        key.parts.joined(separator: "/")
    }

    private func keyFromString(_ string: String) -> KVKey {
        KVKey(parts: string.split(separator: "/", omittingEmptySubsequences: false).map(String.init))
    }

    // MARK: - KVBackend

    public func get(key: KVKey) async throws -> KVEntry? {
        let keyStr = keyString(key)
        let rows = try await client.query(
            "SELECT value, versionstamp FROM score_kv WHERE key = \(keyStr)")
        for try await (value, versionstamp) in rows.decode((Data, Int64).self) {
            return KVEntry(key: key, value: value, versionstamp: UInt64(versionstamp))
        }
        return nil
    }

    public func set(key: KVKey, value: Data) async throws {
        let keyStr = keyString(key)
        let version = Int64(bumpVersion())
        try await client.query(
            """
            INSERT INTO score_kv (key, value, versionstamp)
            VALUES (\(keyStr), \(value), \(version))
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, versionstamp = EXCLUDED.versionstamp
            """)
    }

    public func delete(key: KVKey) async throws {
        let keyStr = keyString(key)
        try await client.query("DELETE FROM score_kv WHERE key = \(keyStr)")
    }

    public func list(prefix: KVKey, limit: Int) async throws -> [KVEntry] {
        let prefixStr = keyString(prefix)
        let pattern = prefixStr + "%"
        let rows = try await client.query(
            "SELECT key, value, versionstamp FROM score_kv WHERE key LIKE \(pattern) ORDER BY key LIMIT \(limit)"
        )
        var entries: [KVEntry] = []
        for try await (keyText, value, versionstamp) in rows.decode((String, Data, Int64).self) {
            let key = keyFromString(keyText)
            guard key.hasPrefix(KVKey(parts: prefix.parts)) else { continue }
            entries.append(KVEntry(key: key, value: value, versionstamp: UInt64(versionstamp)))
        }
        return entries
    }

    public func commitAtomic(_ operations: [AtomicOp]) async throws {
        try await client.withConnection { connection in
            try await connection.query("BEGIN", logger: .init(label: "score.postgres"))

            do {
                for op in operations {
                    if case .check(let key, let expectedVersion) = op {
                        let keyStr = self.keyString(key)
                        let rows = try await connection.query(
                            "SELECT versionstamp FROM score_kv WHERE key = \(keyStr) FOR UPDATE",
                            logger: .init(label: "score.postgres"))
                        var current: UInt64?
                        for try await (v, ) in rows.decode(Int64.self) {
                            current = UInt64(v)
                        }
                        if current != expectedVersion {
                            throw KVError.commitConflict(key: key)
                        }
                    }
                }

                for op in operations {
                    switch op {
                    case .set(let key, let value):
                        let keyStr = self.keyString(key)
                        let version = Int64(self.bumpVersion())
                        try await connection.query(
                            """
                            INSERT INTO score_kv (key, value, versionstamp)
                            VALUES (\(keyStr), \(value), \(version))
                            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, versionstamp = EXCLUDED.versionstamp
                            """,
                            logger: .init(label: "score.postgres"))
                    case .delete(let key):
                        let keyStr = self.keyString(key)
                        try await connection.query(
                            "DELETE FROM score_kv WHERE key = \(keyStr)",
                            logger: .init(label: "score.postgres"))
                    case .check:
                        break
                    }
                }

                try await connection.query("COMMIT", logger: .init(label: "score.postgres"))
            } catch {
                _ = try? await connection.query("ROLLBACK", logger: .init(label: "score.postgres"))
                throw error
            }
        }
    }
}
