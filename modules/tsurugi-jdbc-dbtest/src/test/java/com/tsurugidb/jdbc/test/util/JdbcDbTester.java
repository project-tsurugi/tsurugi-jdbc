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
package com.tsurugidb.jdbc.test.util;

import java.lang.reflect.Method;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class JdbcDbTester {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private static final boolean START_END_LOG_INFO = true;

    protected static void logInitStart(Logger log, TestInfo info) {
        if (START_END_LOG_INFO) {
            log.info("init all start");
        } else {
            log.debug("init all start");
        }
    }

    protected static void logInitEnd(Logger log, TestInfo info) {
        if (START_END_LOG_INFO) {
            log.info("init all end");
        } else {
            log.debug("init all end");
        }
    }

    protected void logInitStart(TestInfo info) {
        String displayName = getDisplayName(info);

        if (START_END_LOG_INFO) {
            LOG.info("{} init start", displayName);
        } else {
            LOG.debug("{} init start", displayName);
        }
    }

    protected void logInitEnd(TestInfo info) {
        String displayName = getDisplayName(info);

        if (START_END_LOG_INFO) {
            LOG.info("{} init end", displayName);
        } else {
            LOG.debug("{} init end", displayName);
        }
    }

    @BeforeEach
    void tetsterBeforeEach(TestInfo info) {
        String displayName = getDisplayName(info);

        if (START_END_LOG_INFO) {
            LOG.info("{} start", displayName);
        } else {
            LOG.debug("{} start", displayName);
        }
    }

    @AfterEach
    void testerAfterEach(TestInfo info) {
        String displayName = getDisplayName(info);
        if (START_END_LOG_INFO) {
            LOG.info("{} end", displayName);
        } else {
            LOG.debug("{} end", displayName);
        }
    }

    private static String getDisplayName(TestInfo info) {
        String d = info.getDisplayName();
        String m = info.getTestMethod().map(Method::getName).orElse(null);
        if (m != null && !d.startsWith(m)) {
            return m + "() " + d;
        }
        return d;
    }
}
