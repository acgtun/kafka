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

import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Decides when a broker on ISR hold can be released.
 *
 * Whether a broker is on ISR hold is part of its registration in the metadata log. This class
 * only keeps what the active controller needs to decide when to release it: since when it has
 * tracked the hold, how many out-of-sync partitions the broker last reported in a heartbeat, and
 * since when that number has been low enough. This is soft state: a newly active controller
 * starts tracking again from the time it becomes active.
 *
 * This class is not thread-safe. It is only used from the controller event thread.
 */
class IsrHoldManager {
    static final int UNKNOWN_OUT_OF_SYNC_PARTITIONS = -1;

    private static final long NOT_SET = -1L;

    private static final class HeldBroker {
        private final long trackedSinceNs;
        private int reportedOutOfSyncPartitions = UNKNOWN_OUT_OF_SYNC_PARTITIONS;
        private long caughtUpSinceNs = NOT_SET;

        private HeldBroker(long trackedSinceNs) {
            this.trackedSinceNs = trackedSinceNs;
        }
    }

    private final Logger log;
    private final QuorumControllerMetrics metrics;
    private final Map<Integer, HeldBroker> heldBrokers = new HashMap<>();

    IsrHoldManager(LogContext logContext, QuorumControllerMetrics metrics) {
        this.log = logContext.logger(IsrHoldManager.class);
        this.metrics = metrics;
    }

    Set<Integer> trackedBrokers() {
        return Set.copyOf(heldBrokers.keySet());
    }

    /**
     * Make the tracked brokers match the brokers that are on ISR hold in the metadata.
     *
     * @param brokersOnHold The brokers whose registration is on ISR hold.
     * @param nowNs         The current time in monotonic nanoseconds.
     */
    void reconcile(Set<Integer> brokersOnHold, long nowNs) {
        heldBrokers.keySet().retainAll(brokersOnHold);
        for (int brokerId : brokersOnHold) {
            if (heldBrokers.putIfAbsent(brokerId, new HeldBroker(nowNs)) == null) {
                log.info("Tracking broker {}, which is on ISR hold.", brokerId);
            }
        }
        metrics.setIsrHoldBrokerCount(heldBrokers.size());
    }

    /**
     * Record the number of out-of-sync partitions that a broker reported in a heartbeat. The
     * report is ignored if the broker is not tracked.
     *
     * @param brokerId              The broker ID.
     * @param outOfSyncPartitions   The reported number, or -1 if the broker did not report one.
     */
    void report(int brokerId, int outOfSyncPartitions) {
        HeldBroker broker = heldBrokers.get(brokerId);
        if (broker != null) {
            broker.reportedOutOfSyncPartitions = outOfSyncPartitions;
        }
    }

    /**
     * Stop tracking every broker.
     */
    void clear() {
        heldBrokers.clear();
        metrics.setIsrHoldBrokerCount(0);
    }

    /**
     * Find the tracked brokers that should be released from ISR hold: every broker if ISR hold
     * is not active, otherwise the brokers that have reported few enough out-of-sync partitions
     * for long enough, and the brokers that have been tracked for the maximum duration. The
     * returned brokers are no longer tracked.
     *
     * @param config    The current soft fencing configuration.
     * @param active    True if ISR hold is enabled and supported.
     * @param nowNs     The current time in monotonic nanoseconds.
     *
     * @return          The brokers to release, in broker ID order, with the reason for each.
     */
    Map<Integer, String> brokersToRelease(SoftFencingConfig config, boolean active, long nowNs) {
        Map<Integer, String> released = new TreeMap<>();
        long exitStableNs = TimeUnit.MILLISECONDS.toNanos(config.exitStableMs());
        long maxDurationNs = TimeUnit.MILLISECONDS.toNanos(config.maxDurationMs());
        Iterator<Map.Entry<Integer, HeldBroker>> iterator = heldBrokers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, HeldBroker> entry = iterator.next();
            HeldBroker broker = entry.getValue();
            String reason = null;
            if (!active) {
                reason = "ISR hold is not enabled";
            } else if (broker.reportedOutOfSyncPartitions != UNKNOWN_OUT_OF_SYNC_PARTITIONS &&
                    broker.reportedOutOfSyncPartitions <= config.exitOutOfSyncPartitions()) {
                if (broker.caughtUpSinceNs == NOT_SET) {
                    broker.caughtUpSinceNs = nowNs;
                }
                if (nowNs - broker.caughtUpSinceNs >= exitStableNs) {
                    reason = "it has caught up";
                }
            } else {
                broker.caughtUpSinceNs = NOT_SET;
            }
            if (reason == null && maxDurationNs > 0 && nowNs - broker.trackedSinceNs >= maxDurationNs) {
                reason = "it has been on ISR hold for " + config.maxDurationMs() + " ms";
            }
            if (reason != null) {
                iterator.remove();
                released.put(entry.getKey(), reason);
            }
        }
        metrics.setIsrHoldBrokerCount(heldBrokers.size());
        return released;
    }
}
