// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "ScorePostgres",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(name: "ScorePostgres", targets: ["ScorePostgres"]),
    ],
    dependencies: [
        .package(url: "https://github.com/allegro-systems/score.git", branch: "main"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.0"),
    ],
    targets: [
        .target(
            name: "ScorePostgres",
            dependencies: [
                .product(name: "Score", package: "Score"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "ScorePostgresTests",
            dependencies: ["ScorePostgres"]
        ),
    ]
)
