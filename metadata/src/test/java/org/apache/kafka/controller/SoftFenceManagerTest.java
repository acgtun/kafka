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
package org.apache.kafka.controller;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SoftFenceManagerTest {
    private static final SoftFencingConfig CONFIG = new SoftFencingConfig(true, 0, 1000L, -1L, 0L);

    private final QuorumControllerMetrics metrics =
        new QuorumControllerMetrics(Optional.empty(), new MockTime(), 0);

    private SoftFenceManager newManager() {
        return new SoftFenceManager(new LogContext(), metrics);
    }

    private static long ms(long ms) {
        return TimeUnit.MILLISECONDS.toNanos(ms);
    }

    @Test
    public void testSoftFenceAndRemove() {
        SoftFenceManager manager = newManager();
        assertFalse(manager.isSoftFenced(1));
        assertTrue(manager.softFence(1, ms(0), "test"));
        assertFalse(manager.softFence(1, ms(10), "test"));
        assertTrue(manager.softFence(2, ms(10), "test"));
        assertTrue(manager.isSoftFenced(1));
        assertEquals(Set.of(1, 2), manager.softFencedBrokers());
        assertEquals(2, manager.softFencedBrokerCount());
        assertEquals(2, metrics.softFencedBrokerCount());

        assertTrue(manager.remove(1, "test"));
        assertFalse(manager.remove(1, "test"));
        assertFalse(manager.isSoftFenced(1));
        assertEquals(1, metrics.softFencedBrokerCount());
        manager.clear("test");
        assertEquals(Set.of(), manager.softFencedBrokers());
        assertEquals(0, metrics.softFencedBrokerCount());
    }

    @Test
    public void testLiftAfterExitConditionIsStable() {
        SoftFenceManager manager = newManager();
        manager.softFence(1, ms(0), "test");
        manager.softFence(2, ms(0), "test");
        Set<Integer> caughtUp = new HashSet<>();

        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(5000), caughtUp::contains));
        caughtUp.add(1);
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(6000), caughtUp::contains));
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(6999), caughtUp::contains));
        assertEquals(List.of(1), manager.maybeLift(CONFIG, ms(7000), caughtUp::contains));
        assertEquals(Set.of(2), manager.softFencedBrokers());
        assertEquals(1, metrics.softFencedBrokerCount());
    }

    @Test
    public void testFallingBehindRestartsTheStableTimer() {
        SoftFenceManager manager = newManager();
        manager.softFence(1, ms(0), "test");
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(100), b -> true));
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(900), b -> false));
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(1200), b -> true));
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(2100), b -> true));
        assertEquals(List.of(1), manager.maybeLift(CONFIG, ms(2200), b -> true));
    }

    @Test
    public void testLiftImmediatelyWithoutStableTime() {
        SoftFenceManager manager = newManager();
        SoftFencingConfig config = new SoftFencingConfig(true, 0, 0L, -1L, 0L);
        manager.softFence(1, ms(0), "test");
        assertEquals(List.of(), manager.maybeLift(config, ms(1), b -> false));
        assertEquals(List.of(1), manager.maybeLift(config, ms(2), b -> true));
    }

    @Test
    public void testLiftAfterMaxDuration() {
        SoftFenceManager manager = newManager();
        SoftFencingConfig config = new SoftFencingConfig(true, 0, 1000L, -1L, 5000L);
        manager.softFence(1, ms(0), "test");
        manager.softFence(2, ms(3000), "test");
        assertEquals(List.of(), manager.maybeLift(config, ms(4999), b -> false));
        assertEquals(List.of(1), manager.maybeLift(config, ms(5000), b -> false));
        assertEquals(List.of(), manager.maybeLift(config, ms(7999), b -> false));
        assertEquals(List.of(2), manager.maybeLift(config, ms(8000), b -> false));

        // Without a maximum duration a broker that never catches up stays soft-fenced.
        manager.softFence(3, ms(0), "test");
        assertEquals(List.of(), manager.maybeLift(CONFIG, ms(Long.MAX_VALUE / 2_000_000), b -> false));
    }
}
