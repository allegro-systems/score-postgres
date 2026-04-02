import Foundation
import Logging
import PostgresNIO
import ScoreData

/// A PostgreSQL-backed storage backend for `DBStore`.
///
/// Stores records as JSON blobs in PostgreSQL tables with extracted
/// index columns for efficient filtering. Each table has an `id TEXT
/// PRIMARY KEY`, a `data BYTEA NOT NULL`, an `updated_at TIMESTAMPTZ`,
/// plus any index columns defined by the `TableDefinition`.
///
/// ### Setup
///
/// ```swift
/// let client = PostgresClient(configuration: ...)
/// let backend = PostgresDBBackend(client: client)
/// let store = DBStore(backend: backend)
/// ```
public final class PostgresDBBackend: DBBackend, @unchecked Sendable {

    private let client: PostgresClient
    private let lock = NSLock()
    private var pendingTables: [String: [RawColumn]] = [:]
    private var createdTables: Set<String> = []

    /// Creates a backend using the given PostgresNIO client.
    ///
    /// The client must have its `run()` method active in a background task
    /// for queries to work. Tables are created lazily on first use.
    ///
    /// - Parameter client: A running `PostgresClient` instance.
    public init(client: PostgresClient) {
        self.client = client
    }

    // MARK: - Table Management

    /// Registers a table schema for lazy creation.
    ///
    /// The actual `CREATE TABLE` statement runs on the first async
    /// operation that touches this table.
    public func createTable(name: String, columns: [RawColumn]) throws {
        lock.lock()
        defer { lock.unlock() }
        pendingTables[name] = columns
    }

    /// Ensures the table exists in PostgreSQL, creating it if needed.
    private func ensureTable(_ name: String) async throws {
        let columns: [RawColumn]? = lock.withLock {
            if createdTables.contains(name) { return nil }
            return pendingTables[name]
        }

        guard let columns else { return }

        var columnDefs = [
            "id TEXT PRIMARY KEY",
            "data BYTEA NOT NULL",
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
        ]
        for col in columns {
            var def = "\(col.name) \(pgType(col.type))"
            if col.unique { def += " UNIQUE" }
            columnDefs.append(def)
        }

        let createSQL = "CREATE TABLE IF NOT EXISTS \(name) (\(columnDefs.joined(separator: ", ")))"
        try await client.query(PostgresQuery(unsafeSQL: createSQL))

        for col in columns where !col.unique {
            let indexSQL = "CREATE INDEX IF NOT EXISTS idx_\(name)_\(col.name) ON \(name)(\(col.name))"
            try await client.query(PostgresQuery(unsafeSQL: indexSQL))
        }

        lock.withLock {
            createdTables.insert(name)
            pendingTables[name] = nil
        }
    }

    /// Maps SQLite type names to PostgreSQL equivalents.
    private func pgType(_ sqliteType: String) -> String {
        switch sqliteType.uppercased() {
        case "TEXT": return "TEXT"
        case "INTEGER": return "BIGINT"
        case "REAL": return "DOUBLE PRECISION"
        case "BLOB": return "BYTEA"
        default: return "TEXT"
        }
    }

    // MARK: - DBBackend

    public func upsert(table: String, id: String, data: Data, columns: [String: String?]) async throws {
        try await ensureTable(table)

        let colNames = columns.keys.sorted()
        let allCols = ["id", "data", "updated_at"] + colNames

        var placeholders: [String] = []
        for i in 1...allCols.count {
            placeholders.append("$\(i)")
        }

        let updates = (["data", "updated_at"] + colNames).map { "\($0) = EXCLUDED.\($0)" }

        let sql = """
            INSERT INTO \(table) (\(allCols.joined(separator: ", ")))
            VALUES (\(placeholders.joined(separator: ", ")))
            ON CONFLICT(id) DO UPDATE SET \(updates.joined(separator: ", "))
            """

        var bindings = PostgresBindings()
        bindings.append(id)
        try bindings.append(data)
        bindings.append(ISO8601DateFormatter().string(from: Date()))
        for name in colNames {
            if let value = columns[name] ?? nil {
                bindings.append(value)
            } else {
                bindings.appendNull()
            }
        }

        try await client.query(PostgresQuery(unsafeSQL: sql, binds: bindings))
    }

    public func selectOne(table: String, id: String) async throws -> Data? {
        try await ensureTable(table)

        let rows = try await client.query("SELECT data FROM \(unescaped: table) WHERE id = \(id)")
        for try await (data,) in rows.decode(Data.self) {
            return data
        }
        return nil
    }

    public func selectAll(
        table: String,
        filters: [QueryFilter],
        orderBy: String?,
        ascending: Bool,
        limit: Int?
    ) async throws -> [Data] {
        try await ensureTable(table)

        var sql = "SELECT data FROM \(table)"
        var bindings = PostgresBindings()
        var paramIndex = 1

        if !filters.isEmpty {
            var clauses: [String] = []
            for filter in filters {
                switch filter {
                case .equals(let column, let value):
                    clauses.append("\(column) = $\(paramIndex)")
                    bindings.append(value)
                    paramIndex += 1
                case .like(let column, let pattern):
                    clauses.append("\(column) LIKE $\(paramIndex)")
                    bindings.append(pattern)
                    paramIndex += 1
                }
            }
            sql += " WHERE " + clauses.joined(separator: " AND ")
        }

        if let orderBy {
            sql += " ORDER BY \(orderBy) \(ascending ? "ASC" : "DESC")"
        }
        if let limit {
            sql += " LIMIT \(limit)"
        }

        let rows = try await client.query(PostgresQuery(unsafeSQL: sql, binds: bindings))
        var results: [Data] = []
        for try await (data,) in rows.decode(Data.self) {
            results.append(data)
        }
        return results
    }

    public func selectCount(table: String, filters: [QueryFilter]) async throws -> Int {
        try await ensureTable(table)

        var sql = "SELECT COUNT(*) FROM \(table)"
        var bindings = PostgresBindings()
        var paramIndex = 1

        if !filters.isEmpty {
            var clauses: [String] = []
            for filter in filters {
                switch filter {
                case .equals(let column, let value):
                    clauses.append("\(column) = $\(paramIndex)")
                    bindings.append(value)
                    paramIndex += 1
                case .like(let column, let pattern):
                    clauses.append("\(column) LIKE $\(paramIndex)")
                    bindings.append(pattern)
                    paramIndex += 1
                }
            }
            sql += " WHERE " + clauses.joined(separator: " AND ")
        }

        let rows = try await client.query(PostgresQuery(unsafeSQL: sql, binds: bindings))
        for try await (count,) in rows.decode(Int.self) {
            return count
        }
        return 0
    }

    public func deleteOne(table: String, id: String) async throws {
        try await ensureTable(table)
        try await client.query("DELETE FROM \(unescaped: table) WHERE id = \(id)")
    }
}
