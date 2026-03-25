import Testing

@testable import ScorePostgres

@Suite("PostgresKVConfig")
struct PostgresKVConfigTests {

    @Test("Default config uses standard values")
    func defaultConfig() {
        let config = PostgresKVConfig()
        #expect(config.host == "localhost")
        #expect(config.port == 5432)
        #expect(config.username == "postgres")
        #expect(config.tableName == "score_kv")
    }

    @Test("Custom config overrides all fields")
    func customConfig() {
        let config = PostgresKVConfig(
            host: "db.example.com",
            port: 5433,
            username: "app",
            password: "secret",
            database: "production",
            tableName: "custom_kv"
        )
        #expect(config.host == "db.example.com")
        #expect(config.port == 5433)
        #expect(config.username == "app")
        #expect(config.password == "secret")
        #expect(config.database == "production")
        #expect(config.tableName == "custom_kv")
    }
}
