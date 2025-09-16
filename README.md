# Tsurugi JDBC

Tsurugi JDBC is a JDBC library that executes SQL on Tsurugi database.

## Requirements

* Java `>= 11`

* dependent modules:
  * [Tsubakuro](https://github.com/project-tsurugi/tsubakuro)

## How to use

Tsurugi JDBC is hosted on Maven Central Repository.

* https://central.sonatype.com/artifact/com.tsurugidb.jdbc/tsurugi-jdbc/overview

To use on Gradle, add Tsurugi JDBC library to dependencies.

```
dependencies {
    implementation 'com.tsurugidb.jdbc:tsurugi-jdbc:0.1.0'
}
```

The JDBC URL begins with `jdbc:tsurugi:` and specifies the Tsurugi endpoint.  
For example, `jdbc:tsurugi:tcp://localhost:12345` or `jdbc:tsurugi:ipc:tsurugi`.

## How to build

```bash
cd tsurugi-jdbc
./gradlew build
```

### Build with Tsubakuro that installed locally

First, check out and install Tsubakuro locally, and build Tsurugi JDBC with Gradle Property `mavenLocal` .

```bash
cd tsubakuro
./gradlew PublishToMavenLocal -PskipBuildNative

cd tsurugi-jdbc
./gradlew build -PmavenLocal
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)