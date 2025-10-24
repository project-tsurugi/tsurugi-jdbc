# tsurugi-jdbc-dbtest

Tsurugi JDBC test for connecting to [Tsurugi](https://github.com/project-tsurugi/tsurugidb).

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * tsurugi-jdbc
  * [Iceaxe](https://github.com/project-tsurugi/iceaxe)

## How to execute

### Execute

```bash
cd tsurugi-jdbc/modules/tsurugi-jdbc-dbtest
../../gradlew dbtest
```

### Execute with Iceaxe, Tsubakuro that installed locally

Execute with Gradle Property `mavenLocal` .

```bash
cd tsurugi-jdbc/modules/tsurugi-jdbc-dbtest
../../gradlew dbtest -PmavenLocal
```

### Execute with endpoint

```bash
../../gradlew dbtest -Pdbtest.endpoint=tcp://localhost:12345
```

### Execute with credential

```bash
../../gradlew dbtest \
-Pdbtest.user=user \
-Pdbtest.password=password \
-Pdbtest.auth-token=token \
-Pdbtest.credentials=/path/to/credential-file
```

For tests other than credential, specifying only one of `user`, `auth-token`, or `credentials` is sufficient. If none of these are specified, authentication will be performed using the user `tsurugi`.

In the credential test, anything not specified is skipped.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

