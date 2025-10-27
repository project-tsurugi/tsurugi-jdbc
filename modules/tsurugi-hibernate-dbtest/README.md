# tsurugi-hibernate-dbtest

Tsurugi Hibernate test for connecting to [Tsurugi](https://github.com/project-tsurugi/tsurugidb).

## Requirements

* Java `>= 17`

* access to installed dependent modules:
  * tsurugi-hibernate

## How to execute

### Execute

```bash
cd tsurugi-jdbc/modules/tsurugi-hibernate-dbtest
../../gradlew dbtest
```

### Execute with Iceaxe, Tsubakuro that installed locally

Execute with Gradle Property `mavenLocal` .

```bash
cd tsurugi-jdbc/modules/tsurugi-hibernate-dbtest
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

