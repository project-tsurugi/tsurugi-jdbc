# Tsurugi JDBC

Tsurugi JDBC is a JDBC library that executes SQL on [Tsurugi](https://github.com/project-tsurugi/tsurugidb).

> [!IMPORTANT]
>
> This project is in an early stage and may introduce breaking changes in the future.

## Target

* Tsurugi 1.8.0 or later.
* Java `>= 11`

## How to use

Tsurugi JDBC is hosted on Maven Central Repository.

* https://central.sonatype.com/artifact/com.tsurugidb.jdbc/tsurugi-jdbc/overview

To use on Gradle, add Tsurugi JDBC library to dependencies.

```
dependencies {
    implementation 'com.tsurugidb.jdbc:tsurugi-jdbc:0.2.0'
}
```

The JDBC URL begins with `jdbc:tsurugi:` and specifies the Tsurugi endpoint.  
For example, `jdbc:tsurugi:tcp://localhost:12345` or `jdbc:tsurugi:ipc:tsurugi`.

## Modules

- [tsurugi-jdbc](modules/tsurugi-jdbc)
  - Tsurugi JDBC core
- [tsurugi-jdbc-examples](modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example)
  - Tsurugi JDBC examples
- [tsurugi-hibernate](modules/tsurugi-hibernate)
  - Hibernate Dialect for Tsurugi JDBC

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)