/// Configuration for connecting to a PostgreSQL server.
///
/// ### Example
///
/// ```swift
/// let config = PostgresConfig(
///     host: "localhost",
///     port: 5432,
///     username: "score",
///     password: "secret",
///     database: "myapp"
/// )
/// ```
public struct PostgresConfig: Sendable {

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

    public init(
        host: String = "localhost",
        port: Int = 5432,
        username: String = "postgres",
        password: String? = nil,
        database: String? = nil
    ) {
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
    }
}
