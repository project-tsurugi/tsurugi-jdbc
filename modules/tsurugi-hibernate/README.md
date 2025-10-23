# Tsurugi Hibernate

Tsurugi Hibernate provides Dialect of [Hibernate](https://github.com/hibernate/hibernate-orm) for [Tsurugi](https://github.com/project-tsurugi/tsurugidb).

## Requirements

* Java `>= 17`
* Hibernate 7

## How to use

Tsurugi Hibernate is hosted on Maven Central Repository.

* https://central.sonatype.com/artifact/com.tsurugidb.jdbc/tsurugi-hibernate/overview

To use on Gradle, add Tsurugi Hibernate library to dependencies.

```
dependencies {
    implementation "org.hibernate.orm:hibernate-core:7.1.4.Final"
    implementation "com.tsurugidb.jdbc:tsurugi-jdbc:0.1.0-SNAPSHOT"
    implementation "com.tsurugidb.jdbc:tsurugi-hibernate:0.1.0-SNAPSHOT"
}
```

## Example

```java
import com.tsurugidb.jdbc.TsurugiDataSource;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

var dataSource = new TsurugiDataSource();
dataSource.setEndpoint("tcp://localhost:12345");
dataSource.setUser("user");
dataSource.setPassword("password");

var configuration = new Configuration();
configuration.setProperty("hibernate.dialect", "com.tsurugidb.hibernate.TsurugiDialect");

var serviceRegistry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).applySetting("hibernate.connection.datasource", dataSource).build();
```

## How to build

```bash
cd tsurugi-jdbc/modules/tsurugi-hibernate
../../gradlew build
```

### Install

Build and deploy the java libraries into Maven Local Repository.
```
cd tsurugi-jdbc/modules/tsurugi-hibernate
../../gradlew PublishToMavenLocal
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)