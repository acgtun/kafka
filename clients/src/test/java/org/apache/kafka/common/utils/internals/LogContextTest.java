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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogContextTest {

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
        try {
            ctx.contextMap().put("new.key", "value");
            // Should throw UnsupportedOperationException
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
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
        // MDC should be clean before and after log calls
        assertNull(MDC.get("kafka.node.id"));

        Map<String, String> context = Map.of(
                "kafka.node.id", "42",
                "kafka.component", "TestBroker");
        LogContext ctx = new LogContext("[Test id=42] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        // After creating logger and before logging, MDC should still be clean
        assertNull(MDC.get("kafka.node.id"));

        // Log a message - MDC should be populated during the call and cleaned after
        logger.info("test message");

        // After the log call, MDC should be cleaned up
        assertNull(MDC.get("kafka.node.id"));
        assertNull(MDC.get("kafka.component"));
    }

    @Test
    void testMdcNotPopulatedWithEmptyContextMap() {
        // With no context map, MDC should never be touched
        LogContext ctx = new LogContext("[Test] ");
        Logger logger = ctx.logger(LogContextTest.class);

        MDC.put("existing.key", "existing.value");
        logger.info("test message");

        // Existing MDC entry should be untouched
        assertEquals("existing.value", MDC.get("existing.key"));
        MDC.remove("existing.key");
    }

    @Test
    void testMdcCleanedUpEvenOnLoggerException() {
        // Verify MDC cleanup happens in finally block
        Map<String, String> context = Map.of("kafka.node.id", "1");
        LogContext ctx = new LogContext("[Test] ", context);
        Logger logger = ctx.logger(LogContextTest.class);

        // Log at various levels
        logger.trace("trace msg");
        logger.debug("debug msg");
        logger.info("info msg");
        logger.warn("warn msg");
        logger.error("error msg");

        // MDC should be clean after all calls
        assertNull(MDC.get("kafka.node.id"));
    }

    @Test
    void testEmptyContextMapDoesNotModifyExistingMdc() {
        MDC.put("pre-existing", "value");

        LogContext ctx = new LogContext("[Test] ", Collections.emptyMap());
        Logger logger = ctx.logger(LogContextTest.class);
        logger.info("test message");

        // Pre-existing MDC entry should be preserved
        assertEquals("value", MDC.get("pre-existing"));
        MDC.remove("pre-existing");
    }

    @Test
    void testMultipleLoggersShareContextMap() {
        Map<String, String> context = Map.of("kafka.node.id", "5");
        LogContext ctx = new LogContext("[Broker 5] ", context);

        Logger logger1 = ctx.logger(LogContextTest.class);
        Logger logger2 = ctx.logger("another.logger");

        // Both loggers should use the same context
        logger1.info("from logger 1");
        assertNull(MDC.get("kafka.node.id")); // cleaned up

        logger2.info("from logger 2");
        assertNull(MDC.get("kafka.node.id")); // cleaned up
    }
}
