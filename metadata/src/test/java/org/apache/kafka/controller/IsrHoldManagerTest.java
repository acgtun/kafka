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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IsrHoldManagerTest {
    private static final SoftFencingConfig CONFIG = new SoftFencingConfig(true, 2, 1000L, -1L, 0L, true, 1);
    private static final String CAUGHT_UP = "it has caught up";

    private final QuorumControllerMetrics metrics =
        new QuorumControllerMetrics(Optional.empty(), new MockTime(), 0);

    private IsrHoldManager newManager() {
        return new IsrHoldManager(new LogContext(), metrics);
    }

    private static long ms(long ms) {
        return TimeUnit.MILLISECONDS.toNanos(ms);
    }

    @Test
    public void testReconcile() {
        IsrHoldManager manager = newManager();
        assertEquals(Set.of(), manager.trackedBrokers());
        manager.reconcile(Set.of(1, 2), ms(0));
        assertEquals(Set.of(1, 2), manager.trackedBrokers());
        assertEquals(2, metrics.isrHoldBrokerCount());
        manager.reconcile(Set.of(2, 3, 4), ms(10));
        assertEquals(Set.of(2, 3, 4), manager.trackedBrokers());
        assertEquals(3, metrics.isrHoldBrokerCount());
        manager.clear();
        assertEquals(Set.of(), manager.trackedBrokers());
        assertEquals(0, metrics.isrHoldBrokerCount());
    }

    @Test
    public void testReleaseAfterReportIsStable() {
        IsrHoldManager manager = newManager();
        manager.reconcile(Set.of(1, 2), ms(0));

        // A broker that has not reported anything is not released.
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(5000)));
        manager.report(1, 3);
        manager.report(2, 100);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(6000)));

        manager.report(1, 2);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(7000)));
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(7999)));
        assertEquals(Map.of(1, CAUGHT_UP), manager.brokersToRelease(CONFIG, true, ms(8000)));
        assertEquals(Set.of(2), manager.trackedBrokers());
        assertEquals(1, metrics.isrHoldBrokerCount());
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(9000)));
    }

    @Test
    public void testFallingBehindRestartsTheStableTimer() {
        IsrHoldManager manager = newManager();
        manager.reconcile(Set.of(1), ms(0));
        manager.report(1, 0);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(100)));
        manager.report(1, 3);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(900)));
        manager.report(1, 0);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(1200)));
        // A heartbeat without a report restarts the timer as well.
        manager.report(1, IsrHoldManager.UNKNOWN_OUT_OF_SYNC_PARTITIONS);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(1500)));
        manager.report(1, 1);
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(2000)));
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(2999)));
        assertEquals(Map.of(1, CAUGHT_UP), manager.brokersToRelease(CONFIG, true, ms(3000)));
    }

    @Test
    public void testReportForUntrackedBrokerIsIgnored() {
        IsrHoldManager manager = newManager();
        manager.report(1, 0);
        manager.reconcile(Set.of(1), ms(0));
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(0)));
        assertEquals(Map.of(), manager.brokersToRelease(CONFIG, true, ms(5000)));
    }

    @Test
    public void testReleaseAfterMaxDuration() {
        IsrHoldManager manager = newManager();
        SoftFencingConfig config = new SoftFencingConfig(true, 0, 1000L, -1L, 5000L, true, 2);
        manager.reconcile(Set.of(1), ms(0));
        manager.reconcile(Set.of(1, 2), ms(3000));
        manager.report(1, 10);
        assertEquals(Map.of(), manager.brokersToRelease(config, true, ms(4999)));
        assertEquals(Map.of(1, "it has been on ISR hold for 5000 ms"),
            manager.brokersToRelease(config, true, ms(5000)));
        assertEquals(Map.of(), manager.brokersToRelease(config, true, ms(7999)));
        assertEquals(Map.of(2, "it has been on ISR hold for 5000 ms"),
            manager.brokersToRelease(config, true, ms(8000)));
    }

    @Test
    public void testReleaseEveryBrokerWhenNotActive() {
        IsrHoldManager manager = newManager();
        manager.reconcile(Set.of(2, 1), ms(0));
        manager.report(1, 10);
        assertEquals(Map.of(1, "ISR hold is not enabled", 2, "ISR hold is not enabled"),
            manager.brokersToRelease(CONFIG, false, ms(1)));
        assertEquals(Set.of(), manager.trackedBrokers());
    }
}
