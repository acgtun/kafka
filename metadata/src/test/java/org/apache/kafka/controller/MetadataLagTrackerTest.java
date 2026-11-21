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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataLagTrackerTest {
    private static final long RETENTION_NS = 1000;

    @Test
    public void testBrokerWithNoSamplesHasNoLag() {
        MetadataLagTracker tracker = new MetadataLagTracker();
        assertEquals(0, tracker.updateBrokerLag(1, -1, 100));
        assertEquals(0, tracker.brokerLagNs(1));
        assertEquals(0, tracker.brokerLagNs(2));
    }

    @Test
    public void testLagIsTheAgeOfTheOldestUnappliedOffset() {
        MetadataLagTracker tracker = new MetadataLagTracker();
        tracker.sample(10, 100, RETENTION_NS);
        tracker.sample(20, 200, RETENTION_NS);
        tracker.sample(30, 300, RETENTION_NS);

        // A broker that has applied everything has no lag.
        assertEquals(0, tracker.updateBrokerLag(1, 30, 350));
        assertEquals(0, tracker.updateBrokerLag(1, 35, 350));
        // Offset 30 was first seen at time 300.
        assertEquals(50, tracker.updateBrokerLag(1, 20, 350));
        assertEquals(50, tracker.updateBrokerLag(1, 29, 350));
        // Offsets after 10 were first seen at time 200, and offsets up to 10 at time 100.
        assertEquals(150, tracker.updateBrokerLag(1, 10, 350));
        assertEquals(250, tracker.updateBrokerLag(1, 9, 350));
        assertEquals(250, tracker.updateBrokerLag(2, -1, 350));

        assertEquals(250, tracker.brokerLagNs(1));
        tracker.updateBrokerLag(1, 30, 400);
        assertEquals(0, tracker.brokerLagNs(1));
        assertEquals(250, tracker.brokerLagNs(2));
        tracker.removeBroker(2);
        assertEquals(0, tracker.brokerLagNs(2));
    }

    @Test
    public void testSampleIgnoresAnOffsetThatDidNotAdvance() {
        MetadataLagTracker tracker = new MetadataLagTracker();
        tracker.sample(10, 100, RETENTION_NS);
        tracker.sample(10, 200, RETENTION_NS);
        tracker.sample(5, 300, RETENTION_NS);
        assertEquals(1, tracker.numSamples());
        assertEquals(300, tracker.updateBrokerLag(1, 9, 400));
    }

    @Test
    public void testOldSamplesAreDroppedButOneOlderThanTheRetentionIsKept() {
        MetadataLagTracker tracker = new MetadataLagTracker();
        tracker.sample(10, 0, RETENTION_NS);
        tracker.sample(20, 400, RETENTION_NS);
        tracker.sample(30, 800, RETENTION_NS);
        tracker.sample(40, 1200, RETENTION_NS);
        assertEquals(4, tracker.numSamples());
        // At time 1500 the sample at time 400 is older than the retention, so the one before it goes.
        tracker.sample(50, 1500, RETENTION_NS);
        assertEquals(4, tracker.numSamples());
        // A broker that is behind every sample is still measured against the oldest one kept.
        assertEquals(1100, tracker.updateBrokerLag(1, 0, 1500));

        tracker.sample(50, 5000, RETENTION_NS);
        assertEquals(1, tracker.numSamples());
        assertEquals(3500, tracker.updateBrokerLag(1, 0, 5000));

        tracker.clear();
        assertEquals(0, tracker.numSamples());
        assertEquals(0, tracker.brokerLagNs(1));
    }
}
