# Tsurugi JDBC

Tsurugi JDBC is a JDBC library that executes SQL on [Tsurugi](https://github.com/project-tsurugi/tsurugidb).

## Requirements

* Java `>= 11`

* dependent modules:
  * [Tsubakuro/Java](https://github.com/project-tsurugi/tsubakuro)

## How to build

```bash
cd tsurugi-jdbc/modules/tsurugi-jdbc
../../gradlew build
```

### Build with Tsubakuro/Java that installed locally

First, check out and install Tsubakuro/Java locally, and build Tsurugi JDBC with Gradle Property `mavenLocal` .

```bash
cd tsubakuro
./gradlew PublishToMavenLocal -PskipBuildNative

cd tsurugi-jdbc/modules/tsurugi-jdbc
../../gradlew build -PmavenLocal
```

### Install

Build and deploy the java libraries into Maven Local Repository.
```
cd tsurugi-jdbc/modules/tsurugi-jdbc
../../gradlew PublishToMavenLocal
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)