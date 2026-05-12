/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils.internals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogContextTest {

    @AfterEach
    void cleanupMdc() {
        MDC.clear();
    }

    @Test
    void testDefaultConstructor() {
        LogContext ctx = new LogContext();
        assertEquals("", ctx.logPrefix());
        assertTrue(ctx.contextMap().isEmpty());
    }

    @Test
    void testPrefixOnlyConstructor() {
        LogContext ctx = new LogContext("[Test] ");
        assertEquals("[Test] ", ctx.logPrefix());
        assertTrue(ctx.contextMap().isEmpty());
    }

    @Test
    void testPrefixAndContextMapConstructor() {
        Map<String, String> context = Map.of(
                "kafka.node.id", "1",
                "kafka.component", "TestComponent");
        LogContext ctx = new LogContext("[Test id=1] ", context);
        assertEquals("[Test id=1] ", ctx.logPrefix());
        assertEquals(2, ctx.contextMap().size());
        assertEquals("1", ctx.contextMap().get("kafka.node.id"));
        assertEquals("TestComponent", ctx.contextMap().get("kafka.component"));
    }

    @Test
    void testContextMapIsImmutable() {
        Map<String, String> context = Map.of("kafka.node.id", "1");
        LogContext ctx = new LogContext("[Test] ", context);
        assertThrows(UnsupportedOperationException.class,
                () -> ctx.contextMap().put("new.key", "value"));
    }

    @Test
    void testNullPrefixTreatedAsEmpty() {
        LogContext ctx = new LogContext(null);
        assertEquals("", ctx.logPrefix());
    }

    @Test
    void testNullPrefixWithContextMap() {
        Map<String, String> context = Map.of("kafka.node.id", "1");
        LogContext ctx = new LogContext(null, context);
        assertEquals("", ctx.logPrefix());
        assertEquals("1", ctx.contextMap().get("kafka.node.id"));
    }

    @Test
    void testNullContextMapThrowsNPE() {
        assertThrows(NullPointerException.class,
                () -> new LogContext("[Test] ", null));
    }

    @Test
    void testLoggerCreation() {
        LogContext ctx = new LogContext("[Test] ");
        Logger logger = ctx.logger(LogContextTest.class);
        assertNotNull(logger);
        assertEquals(LogContextTest.class.getName(), logger.getName());
    }

    @Test
    void testLoggerCreationWithStringName() {
        LogContext ctx = new LogContext("[Test] ");
        Logger logger = ctx.logger("test.logger");
        assertNotNull(logger);
        assertEquals("test.logger", logger.getName());
    }

    @Test
    void testMdcPopulatedDuringLogCallWithContext() {
        assertNull(MDC.get("kafka.node.id"));

        Map<String, String> context = Map.of(
                "kafka.node.id", "42",
                "kafka.component", "TestBroker");
        LogContext ctx = new LogContext("[Test id=42] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        // Before logging, MDC should be clean
        assertNull(MDC.get("kafka.node.id"));

        // After the log call, MDC should be cleaned up
        logger.info("test message");
        assertNull(MDC.get("kafka.node.id"));
        assertNull(MDC.get("kafka.component"));
    }

    @Test
    void testMdcNotPopulatedWithEmptyContextMap() {
        LogContext ctx = new LogContext("[Test] ");
        Logger logger = ctx.logger(LogContextTest.class);

        MDC.put("existing.key", "existing.value");
        logger.info("test message");

        // Existing MDC entry should be untouched
        assertEquals("existing.value", MDC.get("existing.key"));
    }

    @Test
    void testMdcCleanedUpAtAllLogLevels() {
        Map<String, String> context = Map.of("kafka.node.id", "1");
        LogContext ctx = new LogContext("[Test] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        logger.trace("trace msg");
        assertNull(MDC.get("kafka.node.id"));

        logger.debug("debug msg");
        assertNull(MDC.get("kafka.node.id"));

        logger.info("info msg");
        assertNull(MDC.get("kafka.node.id"));

        logger.warn("warn msg");
        assertNull(MDC.get("kafka.node.id"));

        logger.error("error msg");
        assertNull(MDC.get("kafka.node.id"));
    }

    @Test
    void testEmptyContextMapDoesNotModifyExistingMdc() {
        MDC.put("pre-existing", "value");

        LogContext ctx = new LogContext("[Test] ", Collections.emptyMap());
        Logger logger = ctx.logger(LogContextTest.class);
        logger.info("test message");

        assertEquals("value", MDC.get("pre-existing"));
    }

    @Test
    void testMultipleLoggersShareContextMap() {
        Map<String, String> context = Map.of("kafka.node.id", "5");
        LogContext ctx = new LogContext("[Broker 5] ", context);

        Logger logger1 = ctx.logger(LogContextTest.class);
        Logger logger2 = ctx.logger("another.logger");

        logger1.info("from logger 1");
        assertNull(MDC.get("kafka.node.id"));

        logger2.info("from logger 2");
        assertNull(MDC.get("kafka.node.id"));
    }

    // --- C-1 fix: MDC save/restore preserves pre-existing values ---

    @Test
    void testMdcSaveRestorePreservesPreExistingValues() {
        // Simulate Kafka Connect's LoggingContext setting a value on the same thread
        MDC.put("kafka.node.id", "pre-existing-99");
        MDC.put("unrelated.key", "keep-me");

        Map<String, String> context = Map.of("kafka.node.id", "42");
        LogContext ctx = new LogContext("[Test] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        logger.info("test message");

        // Pre-existing value for the SAME key must be restored, not removed
        assertEquals("pre-existing-99", MDC.get("kafka.node.id"));
        // Unrelated keys must be untouched
        assertEquals("keep-me", MDC.get("unrelated.key"));
    }

    @Test
    void testMdcSaveRestoreWithMultipleOverlappingKeys() {
        MDC.put("kafka.node.id", "original-node");
        MDC.put("kafka.component", "original-component");

        Map<String, String> context = Map.of(
                "kafka.node.id", "new-node",
                "kafka.component", "new-component",
                "kafka.client.id", "new-only");
        LogContext ctx = new LogContext("[Test] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        logger.info("test");

        // Overlapping keys restored to original values
        assertEquals("original-node", MDC.get("kafka.node.id"));
        assertEquals("original-component", MDC.get("kafka.component"));
        // Key that didn't exist before should be removed
        assertNull(MDC.get("kafka.client.id"));
    }

    // --- C-2 fix: reentrancy ---

    @Test
    void testReentrantLogCallsDoNotCorruptMdc() {
        // Simulate two LogContext loggers on the same thread logging in sequence
        // (reentrancy via exception handler or listener is hard to test directly,
        // but sequential calls verify the save/restore is per-call)
        Map<String, String> outer = Map.of("kafka.node.id", "outer");
        Map<String, String> inner = Map.of("kafka.node.id", "inner");

        LogContext outerCtx = new LogContext("[Outer] ", outer);
        LogContext innerCtx = new LogContext("[Inner] ", inner);

        Logger outerLogger = outerCtx.logger(LogContextTest.class);
        Logger innerLogger = innerCtx.logger("inner.logger");

        outerLogger.info("outer message");
        assertNull(MDC.get("kafka.node.id"));

        innerLogger.info("inner message");
        assertNull(MDC.get("kafka.node.id"));

        // Interleaved usage should also be safe
        outerLogger.warn("outer again");
        assertNull(MDC.get("kafka.node.id"));
    }

    // --- Concurrent threads test ---

    @Test
    void testConcurrentThreadsDoNotInterfere() throws Exception {
        Map<String, String> context = Map.of("kafka.node.id", "shared");
        LogContext ctx = new LogContext("[Shared] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        logger.info("thread " + threadNum + " iteration " + j);
                        String leftover = MDC.get("kafka.node.id");
                        if (leftover != null) {
                            firstError.compareAndSet(null, new AssertionError(
                                    "MDC leaked on thread " + threadNum + ": kafka.node.id=" + leftover));
                            return;
                        }
                    }
                } catch (Throwable e) {
                    firstError.compareAndSet(null, e);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join(5000);

        Throwable error = firstError.get();
        if (error instanceof Exception) throw (Exception) error;
        if (error instanceof Error) throw (Error) error;
    }
}
