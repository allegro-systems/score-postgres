/// Configuration for connecting to a PostgreSQL server.
///
/// ### Example
///
/// ```swift
/// let config = PostgresKVConfig(
///     host: "localhost",
///     port: 5432,
///     username: "score",
///     password: "secret",
///     database: "myapp"
/// )
/// ```
public struct PostgresKVConfig: Sendable {

    /// The PostgreSQL server hostname.
    public let host: String

    /// The PostgreSQL server port.
    public let port: Int

    /// The database username.
    public let username: String

    /// The database password.
    public let password: String?

    /// The database name.
    public let database: String?

    /// The table name for key-value storage.
    public let tableName: String

    public init(
        host: String = "localhost",
        port: Int = 5432,
        username: String = "postgres",
        password: String? = nil,
        database: String? = nil,
        tableName: String = "score_kv"
    ) {
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.tableName = tableName
    }
}
