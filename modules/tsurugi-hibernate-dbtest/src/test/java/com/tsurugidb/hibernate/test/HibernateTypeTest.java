/*
 * Copyright 2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.hibernate.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;

import javax.sql.DataSource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.hibernate.TsurugiDialect;
import com.tsurugidb.hibernate.test.util.HibernateTestConnector;
import com.tsurugidb.hibernate.test.util.HibernateTester;

public class HibernateTypeTest extends HibernateTester {

    private static SessionFactory sessionFactory;
    private Session session;

    @BeforeAll
    static void beforeAll() {
        Configuration configuration = new Configuration();

        DataSource dataSource = HibernateTestConnector.createDataSource();
        configuration.setProperty("hibernate.show_sql", "true");
        configuration.setProperty("hibernate.format_sql", "false");
        configuration.setProperty("hibernate.hbm2ddl.auto", "none");

        configuration.setProperty("hibernate.dialect", TsurugiDialect.class.getCanonicalName());

        StandardServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).applySetting("hibernate.connection.datasource", dataSource).build();
        sessionFactory = configuration.buildSessionFactory(serviceRegistry);
    }

    @AfterAll
    static void afterAll() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

    @BeforeEach
    void before() {
        this.session = sessionFactory.openSession();
    }

    @AfterEach
    void after() {
        if (this.session != null && session.isOpen()) {
            session.close();
        }
    }

    @Test
    void date() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value date" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = LocalDate.of(2025, 10, 23);
        var value2 = LocalDate.of(1970, 1, 1);

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalDate) results.get(0)[1];
            var actual2 = (LocalDate) results.get(1)[1];

            assertEquals(value1, actual1);
            assertEquals(value2, actual2);
        }
    }

    @Test
    void date_sqlDate() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value date" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = java.sql.Date.valueOf(LocalDate.of(2025, 10, 23));
        var value2 = java.sql.Date.valueOf(LocalDate.of(1970, 1, 1));

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalDate) results.get(0)[1];
            var actual2 = (LocalDate) results.get(1)[1];

            assertEquals(value1.toLocalDate(), actual1);
            assertEquals(value2.toLocalDate(), actual2);
        }
    }

    @Test
    void time() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value time" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = LocalTime.of(22, 30, 59, 123_000_000);
        var value2 = LocalTime.of(0, 0, 0, 0);

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalTime) results.get(0)[1];
            var actual2 = (LocalTime) results.get(1)[1];

            assertEquals(value1, actual1);
            assertEquals(value2, actual2);
        }
    }

    @Test
    void time_sqlTime() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value time" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = java.sql.Time.valueOf(LocalTime.of(22, 30, 59, 123_000_000));
        var value2 = java.sql.Time.valueOf(LocalTime.of(0, 0, 0, 0));

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalTime) results.get(0)[1];
            var actual2 = (LocalTime) results.get(1)[1];

            assertEquals(value1.toLocalTime(), actual1);
            assertEquals(value2.toLocalTime(), actual2);
        }
    }

    @Test
    void timestamp() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value timestamp" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = LocalDateTime.of(2025, 10, 23, 22, 30, 59, 123_000_000);
        var value2 = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalDateTime) results.get(0)[1];
            var actual2 = (LocalDateTime) results.get(1)[1];

            assertEquals(value1, actual1);
            assertEquals(value2, actual2);
        }
    }

    @Test
    void timestamp_sqlTimestamp() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value timestamp" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 23, 22, 30, 59, 123_000_000));
        var value2 = java.sql.Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (LocalDateTime) results.get(0)[1];
            var actual2 = (LocalDateTime) results.get(1)[1];

            assertEquals(value1.toLocalDateTime(), actual1);
            assertEquals(value2.toLocalDateTime(), actual2);
        }
    }

    @Test
    void timeWithTimeZone() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value time with time zone" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = OffsetTime.of(22, 30, 59, 123_000_000, ZoneOffset.ofHours(9));
        var value2 = OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC);

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (OffsetTime) results.get(0)[1];
            var actual2 = (OffsetTime) results.get(1)[1];

            assertEquals(value1.withOffsetSameInstant(ZoneOffset.UTC), actual1);
            assertEquals(value2.withOffsetSameInstant(ZoneOffset.UTC), actual2);
        }
    }

    @Test
    void timestampWithTimeZone() {
        { // create table
            Transaction transaction = session.beginTransaction();

            session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
            session.createNativeQuery("create table test (" //
                    + "  pk int primary key, " //
                    + "  value timestamp with time zone" //
                    + ")", Object.class).executeUpdate();

            transaction.commit();
        }
        session.clear();

        var value1 = OffsetDateTime.of(2025, 10, 23, 22, 30, 59, 123_000_000, ZoneOffset.ofHours(9));
        var value2 = OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        { // insert
            Transaction transaction = session.beginTransaction();

            var query1 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 1).setParameter(2, value1);
            query1.executeUpdate();
            var query2 = session.createNativeQuery("insert into test values(?, ?)", Object.class) //
                    .setParameter(1, 2).setParameter(2, value2);
            query2.executeUpdate();

            transaction.commit();
        }
        session.clear();

        { // select
            List<Object[]> results = session.createNativeQuery("select * from test order by pk", Object[].class) //
                    .getResultList();
            assertEquals(2, results.size());

            var actual1 = (OffsetDateTime) results.get(0)[1];
            var actual2 = (OffsetDateTime) results.get(1)[1];

            assertEquals(value1.withOffsetSameInstant(ZoneOffset.UTC), actual1);
            assertEquals(value2.withOffsetSameInstant(ZoneOffset.UTC), actual2);
        }
    }
}
