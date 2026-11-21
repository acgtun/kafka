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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationIsrHoldChange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.SoftFencingVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.config.SoftFencingConfigs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static org.apache.kafka.controller.ReplicationControlTestContext.defaultBrokerEpoch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of ISR hold in the ReplicationControlManager.
 */
@Timeout(40)
public class IsrHoldReplicationControlTest {
    private static ReplicationControlTestContext isrHoldContext(boolean isrHoldEnabled, int featureLevel, int maxBrokers) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "1000").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, Boolean.toString(isrHoldEnabled)).
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG, Integer.toString(maxBrokers)).
            build();
        ctx.featureControl.replay(new FeatureLevelRecord().
            setName(SoftFencingVersion.FEATURE_NAME).
            setFeatureLevel((short) featureLevel));
        return ctx;
    }

    /**
     * Send a heartbeat that unfences a fenced broker and return the records it generates.
     */
    private static List<ApiMessageAndVersion> unfence(ReplicationControlTestContext ctx, int brokerId) {
        ctx.clusterControl.trackBrokerHeartbeat(brokerId, defaultBrokerEpoch(brokerId));
        List<ApiMessageAndVersion> records = ctx.replicationControl.processBrokerHeartbeat(
            new BrokerHeartbeatRequestData().
                setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
                setCurrentMetadataOffset(1).
                setWantFence(false).setWantShutDown(false), 0).records();
        ctx.replay(records);
        return records;
    }

    private static boolean isrHold(ReplicationControlTestContext ctx, int brokerId) {
        return ctx.clusterControl.brokerRegistrations().get(brokerId).isrHold();
    }

    @Test
    public void testBrokerUnfencedWhileOutOfSyncIsPutOnIsrHold() {
        ReplicationControlTestContext ctx = isrHoldContext(true, 1, 1);
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        // A broker with nothing to catch up on is neither soft-fenced nor put on hold.
        assertEquals(Set.of(), ctx.clusterControl.brokersOnIsrHold());
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();

        ctx.fenceBrokers(0);
        assertEquals(OptionalInt.of(1), ctx.currentLeader(new TopicIdPartition(fooId, 0)));

        // One record unfences the broker and puts it on hold.
        assertEquals(List.of(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
            setBrokerId(0).setBrokerEpoch(defaultBrokerEpoch(0)).
            setFenced(BrokerRegistrationFencingChange.UNFENCE.value()).
            setIsrHold(BrokerRegistrationIsrHoldChange.HOLD.value()), (short) 4)), unfence(ctx, 0));
        assertTrue(ctx.clusterControl.isUnfenced(0));
        assertTrue(isrHold(ctx, 0));
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
        assertEquals(Set.of(0), ctx.clusterControl.brokersOnIsrHold());

        // Fencing the broker again clears the hold.
        ctx.fenceBrokers(0);
        assertFalse(isrHold(ctx, 0));
        assertEquals(Set.of(), ctx.clusterControl.brokersOnIsrHold());
    }

    private static void assertNotPutOnIsrHold(ReplicationControlTestContext ctx) {
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});
        ctx.fenceBrokers(0);
        // The broker is unfenced by the same record as without ISR hold.
        assertEquals(List.of(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
            setBrokerId(0).setBrokerEpoch(defaultBrokerEpoch(0)).
            setFenced(BrokerRegistrationFencingChange.UNFENCE.value()), (short) 0)), unfence(ctx, 0));
        assertFalse(isrHold(ctx, 0));
    }

    @Test
    public void testBrokerIsNotPutOnIsrHoldWhenIsrHoldIsDisabled() {
        ReplicationControlTestContext ctx = isrHoldContext(false, 1, 1);
        assertNotPutOnIsrHold(ctx);
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testBrokerIsNotPutOnIsrHoldWhenFeatureIsNotEnabled() {
        ReplicationControlTestContext ctx = isrHoldContext(true, 0, 1);
        assertNotPutOnIsrHold(ctx);
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testBrokerIsNotPutOnIsrHoldWhenSoftFencingIsDisabled() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, "true").
            build();
        ctx.featureControl.replay(new FeatureLevelRecord().
            setName(SoftFencingVersion.FEATURE_NAME).
            setFeatureLevel(SoftFencingVersion.SFV_1.featureLevel()));
        assertNotPutOnIsrHold(ctx);
        assertEquals(Set.of(), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testNumberOfBrokersOnIsrHoldIsLimited() {
        ReplicationControlTestContext ctx = isrHoldContext(true, 1, 2);
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2, 3}});
        ctx.fenceBrokers(0, 1, 2);

        unfence(ctx, 0);
        unfence(ctx, 1);
        unfence(ctx, 2);
        // The third broker is soft-fenced like the others, but only two brokers can be on hold.
        assertEquals(Set.of(0, 1, 2), ctx.replicationControl.softFencedBrokers());
        assertEquals(Set.of(0, 1), ctx.clusterControl.brokersOnIsrHold());

        // Once a held broker is fenced, the next broker that is unfenced can be put on hold.
        ctx.fenceBrokers(0, 2);
        assertEquals(Set.of(1), ctx.clusterControl.brokersOnIsrHold());
        unfence(ctx, 2);
        assertEquals(Set.of(1, 2), ctx.clusterControl.brokersOnIsrHold());
    }
    /**
     * Send a heartbeat in which the broker reports its number of out-of-sync partitions.
     */
    private static void heartbeat(ReplicationControlTestContext ctx, int brokerId, int outOfSyncPartitions) {
        ctx.clusterControl.trackBrokerHeartbeat(brokerId, defaultBrokerEpoch(brokerId));
        ctx.replay(ctx.replicationControl.processBrokerHeartbeat(new BrokerHeartbeatRequestData().
            setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
            setCurrentMetadataOffset(1).
            setWantFence(false).setWantShutDown(false).
            setOutOfSyncPartitions(outOfSyncPartitions), 0).records());
    }

    private static List<ApiMessageAndVersion> releaseRecords(int brokerId) {
        return List.of(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
            setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
            setIsrHold(BrokerRegistrationIsrHoldChange.RELEASE.value()), (short) 4));
    }

    /**
     * Run the periodic check, replay its records and return them.
     */
    private static List<ApiMessageAndVersion> check(ReplicationControlTestContext ctx) {
        List<ApiMessageAndVersion> records = ctx.replicationControl.maybeLiftSoftFences().records();
        ctx.replay(records);
        return records;
    }

    /**
     * Create a context in which broker 0 is on ISR hold and soft-fenced, out of the ISR of one partition.
     */
    private static ReplicationControlTestContext contextWithBrokerOnIsrHold(String... staticConfigs) {
        ReplicationControlTestContext.Builder builder = new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "1000").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, "true");
        for (int i = 0; i < staticConfigs.length; i += 2) {
            builder.setStaticConfig(staticConfigs[i], staticConfigs[i + 1]);
        }
        ReplicationControlTestContext ctx = builder.build();
        ctx.featureControl.replay(new FeatureLevelRecord().
            setName(SoftFencingVersion.FEATURE_NAME).
            setFeatureLevel(SoftFencingVersion.SFV_1.featureLevel()));
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});
        ctx.fenceBrokers(0);
        unfence(ctx, 0);
        assertTrue(isrHold(ctx, 0));
        return ctx;
    }

    @Test
    public void testIsrHoldIsReleasedAfterBrokerReportsItHasCaughtUp() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold();

        // A broker that reports nothing, or that is still behind, stays on hold.
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(5000);
        assertEquals(List.of(), check(ctx));
        heartbeat(ctx, 0, 1);
        ctx.time.sleep(5000);
        assertEquals(List.of(), check(ctx));

        // It must report that it has caught up for the whole stable time.
        heartbeat(ctx, 0, 0);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(500);
        heartbeat(ctx, 0, 1);
        assertEquals(List.of(), check(ctx));
        heartbeat(ctx, 0, 0);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(999);
        heartbeat(ctx, 0, 0);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(1);
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
        assertEquals(List.of(), check(ctx));

        // The broker is still soft-fenced: it has not joined the ISR of its partition yet.
        assertEquals(1, ctx.replicationControl.outOfSyncReplicaCount(0));
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testIsrHoldIsReleasedAfterMaxDuration() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold(
            SoftFencingConfigs.SOFT_FENCING_MAX_DURATION_MS_CONFIG, "60000");
        assertEquals(List.of(), check(ctx));
        heartbeat(ctx, 0, 7);
        ctx.time.sleep(59999);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(1);
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
        assertEquals(Set.of(), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testIsrHoldIsReleasedWhenItIsDisabled() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold();
        assertEquals(List.of(), check(ctx));
        ctx.replay(List.of(new ApiMessageAndVersion(new ConfigRecord().
            setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("").
            setName(SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG).setValue("false"), (short) 0)));
        assertTrue(isrHold(ctx, 0));
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
        // Soft fencing itself is still enabled.
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testIsrHoldIsReleasedWhenSoftFencingIsDisabled() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold();
        ctx.replay(List.of(new ApiMessageAndVersion(new ConfigRecord().
            setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("").
            setName(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG).setValue("false"), (short) 0)));
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
        assertEquals(Set.of(), ctx.replicationControl.softFencedBrokers());
    }

    @Test
    public void testIsrHoldIsReleasedWhenFeatureIsDowngraded() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold();
        ctx.featureControl.replay(new FeatureLevelRecord().
            setName(SoftFencingVersion.FEATURE_NAME).
            setFeatureLevel(SoftFencingVersion.SFV_0.featureLevel()));
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
    }

    @Test
    public void testNewActiveControllerReleasesIsrHold() {
        ReplicationControlTestContext ctx = contextWithBrokerOnIsrHold();
        heartbeat(ctx, 0, 0);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(900);

        // The hold is in the metadata, so it survives a controller failover. The time for which
        // the broker has been caught up starts over.
        ctx.replicationControl.deactivate();
        ctx.replicationControl.activate();
        assertTrue(isrHold(ctx, 0));
        assertEquals(Set.of(0), ctx.replicationControl.softFencedBrokers());
        ctx.time.sleep(100);
        assertEquals(List.of(), check(ctx));
        heartbeat(ctx, 0, 0);
        assertEquals(List.of(), check(ctx));
        ctx.time.sleep(1000);
        assertEquals(releaseRecords(0), check(ctx));
        assertFalse(isrHold(ctx, 0));
    }
}
