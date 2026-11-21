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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Measures how far behind the metadata log each broker is, in time.
 *
 * The active controller records when it saw each metadata offset committed. A broker reports the
 * last offset it has applied in its heartbeat. The lag of the broker is the time since the oldest
 * record it has not applied was seen committed, or 0 if it has applied everything we sampled.
 *
 * This is soft state of the active controller, used from the controller event thread only.
 */
class MetadataLagTracker {
    private record Sample(long offset, long timeNs) { }

    /**
     * Samples in increasing order of both offset and time.
     */
    private final ArrayDeque<Sample> samples = new ArrayDeque<>();

    /**
     * The lag of each broker as of its last heartbeat.
     */
    private final Map<Integer, Long> brokerLagNs = new HashMap<>();

    /**
     * Record that the given offset is committed.
     *
     * @param committedOffset   The last committed metadata offset.
     * @param nowNs             The current time in monotonic nanoseconds.
     * @param retentionNs       How long to keep samples for.
     */
    void sample(long committedOffset, long nowNs, long retentionNs) {
        Sample last = samples.peekLast();
        if (last == null || committedOffset > last.offset) {
            samples.addLast(new Sample(committedOffset, nowNs));
        }
        // Keep one sample that is older than the retention time, so that a broker that is even
        // further behind is still measured as being at least that far behind.
        while (samples.size() > 1) {
            Iterator<Sample> iterator = samples.iterator();
            iterator.next();
            if (nowNs - iterator.next().timeNs < retentionNs) {
                break;
            }
            samples.removeFirst();
        }
    }

    /**
     * Update the lag of a broker from the offset in its heartbeat.
     *
     * @param brokerId          The broker ID.
     * @param appliedOffset     The last metadata offset that the broker has applied.
     * @param nowNs             The current time in monotonic nanoseconds.
     *
     * @return                  The lag of the broker in nanoseconds.
     */
    long updateBrokerLag(int brokerId, long appliedOffset, long nowNs) {
        long lagNs = 0;
        Iterator<Sample> iterator = samples.descendingIterator();
        while (iterator.hasNext()) {
            Sample sample = iterator.next();
            if (sample.offset <= appliedOffset) {
                break;
            }
            lagNs = Math.max(0, nowNs - sample.timeNs);
        }
        brokerLagNs.put(brokerId, lagNs);
        return lagNs;
    }

    /**
     * Return the lag of a broker as of its last heartbeat, in nanoseconds. A broker that has not
     * sent a heartbeat since it was last removed has no lag.
     */
    long brokerLagNs(int brokerId) {
        return brokerLagNs.getOrDefault(brokerId, 0L);
    }

    void removeBroker(int brokerId) {
        brokerLagNs.remove(brokerId);
    }

    void clear() {
        samples.clear();
        brokerLagNs.clear();
    }

    // Visible for testing
    int numSamples() {
        return samples.size();
    }
}
