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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.ParameterExpression;
import jakarta.persistence.criteria.Root;

public class HibernateCriteriaApiTest extends HibernateTester {

    private static SessionFactory sessionFactory;
    private Session session;

    @Entity(name = "TestEntity")
    @Table(name = "test")
    public static class TestEntity {
        @Id
        @Column(name = "foo")
        private Integer foo;

        @Column(name = "bar")
        private Long bar;

        @Column(name = "zzz")
        private String zzz;

        public TestEntity() {
        }

        public TestEntity(int id, long bar, String zzz) {
            this.foo = id;
            this.bar = bar;
            this.zzz = zzz;
        }

        public Integer getFoo() {
            return foo;
        }

        public void setFoo(Integer foo) {
            this.foo = foo;
        }

        public Long getBar() {
            return bar;
        }

        public void setBar(Long bar) {
            this.bar = bar;
        }

        public String getZzz() {
            return zzz;
        }

        public void setZzz(String zzz) {
            this.zzz = zzz;
        }
    }

    @BeforeAll
    static void beforeAll() {
        Configuration configuration = new Configuration();

        DataSource dataSource = HibernateTestConnector.createDataSource();
        configuration.setProperty("hibernate.show_sql", "true");
        configuration.setProperty("hibernate.format_sql", "false");
        configuration.setProperty("hibernate.hbm2ddl.auto", "none");

        configuration.setProperty("hibernate.dialect", TsurugiDialect.class.getCanonicalName());

        configuration.addAnnotatedClass(TestEntity.class);

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
        Transaction transaction = session.beginTransaction();

        session.createNativeQuery("drop table if exists test", Object.class).executeUpdate();
        session.createNativeQuery("create table test (" //
                + "  foo int primary key, " //
                + "  bar bigint, " //
                + "  zzz varchar(10)" //
                + ")", Object.class).executeUpdate();

        transaction.commit();
    }

    @AfterEach
    void after() {
        if (this.session != null && session.isOpen()) {
            session.close();
        }
    }

    @Test
    void select() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(2, 22, "def"));
            session.persist(new TestEntity(3, 33, "ghi"));

            transaction.commit();
        }
        session.clear();

        { // select
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root);

            List<TestEntity> entities = session.createQuery(cq).getResultList();
            assertEquals(3, entities.size());
        }
    }

    @Test
    void selectEmptyResult() {
        { // select
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root);

            List<TestEntity> entities = session.createQuery(cq).getResultList();
            assertEquals(0, entities.size());
        }
    }

    @Test
    void selectWhere() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(2, 22, "def"));
            session.persist(new TestEntity(3, 33, "ghi"));

            transaction.commit();
        }
        session.clear();

        { // select
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root).where(cb.greaterThanOrEqualTo(root.get("bar"), 22));

            List<TestEntity> entities = session.createQuery(cq).getResultList();
            assertEquals(2, entities.size());
        }
    }

    @Test
    void selectOrderBy() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(3, 33, "ghi"));
            session.persist(new TestEntity(2, 22, "def"));

            transaction.commit();
        }
        session.clear();

        { // select ASC
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root).orderBy(cb.asc(root.get("foo")));

            List<TestEntity> entities = session.createQuery(cq).getResultList();
            assertEquals(3, entities.size());
            assertEquals(1, entities.get(0).getFoo());
            assertEquals(3, entities.get(2).getFoo());
        }

        { // select DESC
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root).orderBy(cb.desc(root.get("foo")));

            List<TestEntity> entities = session.createQuery(cq).getResultList();
            assertEquals(3, entities.size());
            assertEquals(3, entities.get(0).getFoo());
            assertEquals(1, entities.get(2).getFoo());
        }
    }

    @Test
    void selectLimit() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(2, 22, "def"));
            session.persist(new TestEntity(3, 33, "ghi"));

            transaction.commit();
        }
        session.clear();

        { // select
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(root).orderBy(cb.asc(root.get("foo")));

            List<TestEntity> entities = session.createQuery(cq).setMaxResults(2).getResultList();
            assertEquals(2, entities.size());

            assertEquals(1, entities.get(0).getFoo());
            assertEquals(2, entities.get(1).getFoo());
        }
    }

    @Test
    void selectParameter() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(2, 22, "def"));
            session.persist(new TestEntity(3, 33, "ghi"));

            transaction.commit();
        }
        session.clear();

        { // select
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<TestEntity> cq = cb.createQuery(TestEntity.class);
            Root<TestEntity> root = cq.from(TestEntity.class);

            ParameterExpression<Long> bar = cb.parameter(Long.class);
            cq.select(root).where(cb.equal(root.get("bar"), bar));

            List<TestEntity> entities = session.createQuery(cq) //
                    .setParameter(bar, 22L).getResultList();
            assertEquals(1, entities.size());

            assertEquals(2, entities.get(0).getFoo());
        }
    }

    @Test
    void selectAggregate() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "abc"));
            session.persist(new TestEntity(2, 22, "def"));
            session.persist(new TestEntity(3, 33, "ghi"));
            session.persist(new TestEntity(4, 34, "jkl"));

            transaction.commit();
        }
        session.clear();

        { // count
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.count(root));

            Long result = session.createQuery(cq).getSingleResult();
            assertEquals(4, result);
        }
        { // min
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.min(root.get("bar")));

            Long result = session.createQuery(cq).getSingleResult();
            assertEquals(11, result);
        }
        { // max
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.max(root.get("bar")));

            Long result = session.createQuery(cq).getSingleResult();
            assertEquals(34, result);
        }
        { // sum
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.sum(root.get("bar")));

            Long result = session.createQuery(cq).getSingleResult();
            assertEquals(100, result);
        }
        { // avg
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Double> cq = cb.createQuery(Double.class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.avg(root.get("bar")));

            Double result = session.createQuery(cq).getSingleResult();
            assertEquals(25, result);
        }
    }

    @Test
    void selectGroupBy() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "a"));
            session.persist(new TestEntity(2, 23, "a"));
            session.persist(new TestEntity(3, 33, "b"));
            session.persist(new TestEntity(4, 41, "c"));
            session.persist(new TestEntity(5, 42, "c"));
            session.persist(new TestEntity(6, 46, "c"));

            transaction.commit();
        }
        session.clear();

        { // count
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.count(root))).groupBy(root.get("zzz")).orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(2L, result.get(0)[1]);
            assertEquals("b", result.get(1)[0]);
            assertEquals(1L, result.get(1)[1]);
            assertEquals("c", result.get(2)[0]);
            assertEquals(3L, result.get(2)[1]);
        }
        { // min
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.min(root.get("bar")))).groupBy(root.get("zzz")).orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(11L, result.get(0)[1]);
            assertEquals("b", result.get(1)[0]);
            assertEquals(33L, result.get(1)[1]);
            assertEquals("c", result.get(2)[0]);
            assertEquals(41L, result.get(2)[1]);
        }
        { // max
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.max(root.get("bar")))).groupBy(root.get("zzz")).orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(23L, result.get(0)[1]);
            assertEquals("b", result.get(1)[0]);
            assertEquals(33L, result.get(1)[1]);
            assertEquals("c", result.get(2)[0]);
            assertEquals(46L, result.get(2)[1]);
        }
        { // sum
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.sum(root.get("bar")))).groupBy(root.get("zzz")).orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(34L, result.get(0)[1]);
            assertEquals("b", result.get(1)[0]);
            assertEquals(33L, result.get(1)[1]);
            assertEquals("c", result.get(2)[0]);
            assertEquals(129L, result.get(2)[1]);
        }
        { // avg
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.avg(root.get("bar")))).groupBy(root.get("zzz")).orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(17d, result.get(0)[1]);
            assertEquals("b", result.get(1)[0]);
            assertEquals(33d, result.get(1)[1]);
            assertEquals("c", result.get(2)[0]);
            assertEquals(43d, result.get(2)[1]);
        }
    }

    @Test
    void selectHaving() {
        { // insert
            Transaction transaction = session.beginTransaction();

            session.persist(new TestEntity(1, 11, "a"));
            session.persist(new TestEntity(2, 23, "a"));
            session.persist(new TestEntity(3, 33, "b"));
            session.persist(new TestEntity(4, 41, "c"));
            session.persist(new TestEntity(5, 42, "c"));
            session.persist(new TestEntity(6, 46, "c"));

            transaction.commit();
        }
        session.clear();

        { // having
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Object[]> cq = cb.createQuery(Object[].class);
            Root<TestEntity> root = cq.from(TestEntity.class);
            cq.select(cb.array(root.get("zzz"), cb.count(root))) //
                    .groupBy(root.get("zzz")) //
                    .having(cb.greaterThan(cb.count(root), 1L)) //
                    .orderBy(cb.asc(root.get("zzz")));

            List<Object[]> result = session.createQuery(cq).getResultList();
            assertEquals(2, result.size());
            assertEquals("a", result.get(0)[0]);
            assertEquals(2L, result.get(0)[1]);
            assertEquals("c", result.get(1)[0]);
            assertEquals(3L, result.get(1)[1]);
        }
    }
}
