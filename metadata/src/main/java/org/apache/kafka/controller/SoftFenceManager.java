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

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

/**
 * Tracks the brokers that are soft-fenced.
 *
 * A soft-fenced broker is unfenced, but the controller elects it as a partition leader only when
 * no other replica can be elected. This is soft state of the active controller: it is not written
 * to the metadata log, and a newly active controller rebuilds it.
 *
 * This class is not thread-safe. It is only used from the controller event thread.
 */
class SoftFenceManager {
    private static final long NOT_SET = -1L;

    private static final class SoftFencedBroker {
        private final long softFencedAtNs;
        private long exitConditionMetAtNs = NOT_SET;

        private SoftFencedBroker(long softFencedAtNs) {
            this.softFencedAtNs = softFencedAtNs;
        }
    }

    private final Logger log;
    private final Map<Integer, SoftFencedBroker> softFencedBrokers = new HashMap<>();

    SoftFenceManager(LogContext logContext) {
        this.log = logContext.logger(SoftFenceManager.class);
    }

    boolean isSoftFenced(int brokerId) {
        return softFencedBrokers.containsKey(brokerId);
    }

    Set<Integer> softFencedBrokers() {
        return Set.copyOf(softFencedBrokers.keySet());
    }

    int softFencedBrokerCount() {
        return softFencedBrokers.size();
    }

    /**
     * Soft-fence a broker.
     *
     * @param brokerId  The broker ID.
     * @param nowNs     The current time in monotonic nanoseconds.
     * @param reason    Why the broker is soft-fenced, for the log message.
     *
     * @return          True if the broker was not soft-fenced before.
     */
    boolean softFence(int brokerId, long nowNs, String reason) {
        if (softFencedBrokers.putIfAbsent(brokerId, new SoftFencedBroker(nowNs)) != null) {
            return false;
        }
        log.info("Soft-fencing broker {}: {}.", brokerId, reason);
        return true;
    }

    /**
     * Remove the soft fence of a broker, if it has one.
     *
     * @param brokerId  The broker ID.
     * @param reason    Why the soft fence is removed, for the log message.
     *
     * @return          True if the broker was soft-fenced.
     */
    boolean remove(int brokerId, String reason) {
        if (softFencedBrokers.remove(brokerId) == null) {
            return false;
        }
        log.info("Lifting the soft fence of broker {}: {}.", brokerId, reason);
        return true;
    }

    /**
     * Remove every soft fence.
     *
     * @param reason    Why the soft fences are removed, for the log message.
     */
    void clear(String reason) {
        if (!softFencedBrokers.isEmpty()) {
            log.info("Lifting the soft fence of brokers {}: {}.", softFencedBrokers.keySet(), reason);
            softFencedBrokers.clear();
        }
    }

    /**
     * Lift the soft fence of every broker that has met the exit condition for long enough, or
     * that has been soft-fenced for the maximum duration.
     *
     * @param config            The current soft fencing configuration.
     * @param nowNs             The current time in monotonic nanoseconds.
     * @param isExitConditionMet Returns true if the given soft-fenced broker has caught up.
     *
     * @return                  The brokers whose soft fence was lifted.
     */
    List<Integer> maybeLift(SoftFencingConfig config, long nowNs, IntPredicate isExitConditionMet) {
        List<Integer> lifted = new ArrayList<>();
        long exitStableNs = TimeUnit.MILLISECONDS.toNanos(config.exitStableMs());
        long maxDurationNs = TimeUnit.MILLISECONDS.toNanos(config.maxDurationMs());
        Iterator<Map.Entry<Integer, SoftFencedBroker>> iterator = softFencedBrokers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, SoftFencedBroker> entry = iterator.next();
            int brokerId = entry.getKey();
            SoftFencedBroker broker = entry.getValue();
            String reason = null;
            if (isExitConditionMet.test(brokerId)) {
                if (broker.exitConditionMetAtNs == NOT_SET) {
                    broker.exitConditionMetAtNs = nowNs;
                }
                if (nowNs - broker.exitConditionMetAtNs >= exitStableNs) {
                    reason = "it has caught up";
                }
            } else {
                broker.exitConditionMetAtNs = NOT_SET;
            }
            if (reason == null && maxDurationNs > 0 && nowNs - broker.softFencedAtNs >= maxDurationNs) {
                reason = "it has been soft-fenced for " + config.maxDurationMs() + " ms";
            }
            if (reason != null) {
                iterator.remove();
                lifted.add(brokerId);
                log.info("Lifting the soft fence of broker {}: {}.", brokerId, reason);
            }
        }
        return lifted;
    }
}
