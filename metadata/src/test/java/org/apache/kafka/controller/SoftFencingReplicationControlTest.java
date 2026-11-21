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

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.config.SoftFencingConfigs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.PREFERRED_LEADER_NOT_AVAILABLE;
import static org.apache.kafka.controller.ReplicationControlTestContext.defaultBrokerEpoch;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of soft fencing in the ReplicationControlManager.
 */
@Timeout(40)
public class SoftFencingReplicationControlTest {
    private static ReplicationControlTestContext softFencingContext() {
        return new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "1000").
            build();
    }

    private static ElectLeadersResponseData electPreferredLeader(
        ReplicationControlManager replication,
        String topic,
        int partition
    ) {
        ElectLeadersRequestData request = new ElectLeadersRequestData().
            setElectionType(ElectionType.PREFERRED.value);
        request.topicPartitions().add(new TopicPartitions().setTopic(topic).setPartitions(List.of(partition)));
        return replication.electLeaders(request).response();
    }

    private static List<BrokerState> isrWithDefaultEpoch(Integer... isr) {
        return Arrays.stream(isr).
            map(brokerId -> new BrokerState().setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId))).
            collect(Collectors.toList());
    }

    @Test
    public void testSoftFencedBrokerIsNotElectedUntilItCatchesUp() {
        ReplicationControlTestContext ctx = softFencingContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        // A broker with nothing to catch up on is not soft-fenced.
        assertEquals(Set.of(), replication.softFencedBrokers());
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {0, 1, 2}, new int[] {1, 2, 0}}).topicId();
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0);
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1);

        ctx.fenceBrokers(0);
        assertEquals(OptionalInt.of(1), ctx.currentLeader(foo0));
        assertEquals(Set.of(), replication.softFencedBrokers());

        // The broker comes back while it is out of the ISR of both partitions.
        ctx.unfenceBrokers(0);
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // It rejoins the ISR of the partition it is the preferred leader of, but not of the other.
        ctx.alterPartition(foo0, 1, isrWithDefaultEpoch(1, 2, 0), LeaderRecoveryState.RECOVERED);
        assertEquals(1, replication.outOfSyncReplicaCount(0));
        ctx.time.sleep(5000);
        ctx.replay(replication.maybeLiftSoftFences().records());
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // Neither the periodic nor the requested preferred election moves leadership to it.
        ControllerResult<Boolean> balanceResult = replication.maybeBalancePartitionLeaders();
        assertEquals(List.of(), balanceResult.records());
        assertEquals(PREFERRED_LEADER_NOT_AVAILABLE.code(), electPreferredLeader(replication, "foo", 0).
            replicaElectionResults().get(0).partitionResult().get(0).errorCode());
        assertEquals(OptionalInt.of(1), ctx.currentLeader(foo0));

        // Once it has caught up, the soft fence is lifted after the stable time.
        ctx.alterPartition(foo1, 1, isrWithDefaultEpoch(1, 2, 0), LeaderRecoveryState.RECOVERED);
        assertEquals(0, replication.outOfSyncReplicaCount(0));
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.time.sleep(999);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.time.sleep(1);
        assertEquals(ControllerResult.of(List.of(), false), replication.maybeLiftSoftFences());
        assertEquals(Set.of(), replication.softFencedBrokers());

        // The next preferred election gives it its leadership back.
        ctx.replay(replication.maybeBalancePartitionLeaders().records());
        assertEquals(OptionalInt.of(0), ctx.currentLeader(foo0));
    }

    @Test
    public void testSoftFencedBrokerIsElectedAsLastChoice() {
        ReplicationControlTestContext ctx = softFencingContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {0, 1, 2}, new int[] {1, 2, 0}, new int[] {2, 0, 1}}).topicId();
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0);
        TopicIdPartition foo2 = new TopicIdPartition(fooId, 2);

        ctx.fenceBrokers(0);
        ctx.unfenceBrokers(0);
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.alterPartition(foo0, 1, isrWithDefaultEpoch(1, 2, 0), LeaderRecoveryState.RECOVERED);
        ctx.alterPartition(foo2, 2, isrWithDefaultEpoch(2, 1, 0), LeaderRecoveryState.RECOVERED);

        // When the leader of foo-0 goes away, broker 2 is elected ahead of broker 0, even though
        // broker 0 comes first in the replica list.
        QuorumControllerMetrics metrics = ctx.clusterControl.metrics();
        assertEquals(1, metrics.softFencedBrokerCount());
        assertEquals(0, metrics.softFencedReplicasSkipped());
        ctx.fenceBrokers(1);
        assertEquals(OptionalInt.of(2), ctx.currentLeader(foo0));
        assertEquals(Set.of(0), replication.softFencedBrokers());
        assertEquals(1, metrics.softFencedReplicasSkipped());
        assertEquals(0, metrics.softFencedLeaderElections());

        // When broker 2 goes away too, broker 0 is the only choice and is elected.
        ctx.fenceBrokers(Set.of(1, 2));
        assertEquals(OptionalInt.of(0), ctx.currentLeader(foo0));
        assertEquals(OptionalInt.of(0), ctx.currentLeader(foo2));
        assertArrayEquals(new int[] {0}, replication.getPartition(fooId, 0).isr);
        assertEquals(1, metrics.softFencedReplicasSkipped());
        assertEquals(2, metrics.softFencedLeaderElections());
    }

    @Test
    public void testSoftFencingIsDisabledByDefault() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0);
        ctx.fenceBrokers(0);
        ctx.unfenceBrokers(0);
        assertEquals(1, replication.outOfSyncReplicaCount(0));
        assertEquals(Set.of(), replication.softFencedBrokers());
        replication.activate();
        assertEquals(Set.of(), replication.softFencedBrokers());
        ctx.alterPartition(foo0, 1, isrWithDefaultEpoch(1, 2, 0), LeaderRecoveryState.RECOVERED);
        ctx.replay(replication.maybeBalancePartitionLeaders().records());
        assertEquals(OptionalInt.of(0), ctx.currentLeader(foo0));
    }

    @Test
    public void testSoftFencesAreRebuiltOnActivationAndClearedWhenDisabled() {
        ReplicationControlTestContext ctx = softFencingContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}, new int[] {1, 2, 0}});
        ctx.fenceBrokers(Set.of(0, 3));
        ctx.unfenceBrokers(0);
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // A controller that loses and regains leadership soft-fences the unfenced brokers that are
        // out of sync. Broker 3 is fenced and brokers 1 and 2 are in sync.
        replication.deactivate();
        assertEquals(Set.of(), replication.softFencedBrokers());
        replication.activate();
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // Fencing the broker drops the soft fence.
        ctx.fenceBrokers(Set.of(0, 3));
        assertEquals(Set.of(), replication.softFencedBrokers());
        ctx.unfenceBrokers(0);
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // Turning the feature off lifts every soft fence at the next check.
        ctx.replay(List.of(new ApiMessageAndVersion(new ConfigRecord().
            setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("").
            setName(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG).setValue("false"), (short) 0)));
        assertEquals(Set.of(0), replication.softFencedBrokers());
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(), replication.softFencedBrokers());
    }

    @Test
    public void testSoftFenceIsLiftedAfterMaxDuration() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_MAX_DURATION_MS_CONFIG, "60000").
            build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}});
        ctx.fenceBrokers(0);
        ctx.unfenceBrokers(0);
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.time.sleep(59999);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.time.sleep(1);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(), replication.softFencedBrokers());
        assertEquals(1, replication.outOfSyncReplicaCount(0));
    }

    private static void heartbeat(ReplicationControlTestContext ctx, int brokerId, long metadataOffset) {
        ctx.clusterControl.trackBrokerHeartbeat(brokerId, defaultBrokerEpoch(brokerId));
        ctx.replay(ctx.replicationControl.processBrokerHeartbeat(new BrokerHeartbeatRequestData().
            setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
            setCurrentMetadataOffset(metadataOffset).
            setWantFence(false).setWantShutDown(false), 0).records());
    }

    @Test
    public void testBrokerWithLaggingMetadataIsSoftFenced() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "1000").
            setStaticConfig(SoftFencingConfigs.SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG, "5000").
            build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0);

        replication.sampleCommittedMetadataOffset(10);
        heartbeat(ctx, 0, 10);
        heartbeat(ctx, 1, 10);
        ctx.time.sleep(3000);
        replication.sampleCommittedMetadataOffset(20);
        heartbeat(ctx, 0, 10);
        heartbeat(ctx, 1, 20);
        // Broker 0 has not applied offset 20, which was committed 3 seconds ago.
        ctx.time.sleep(3000);
        replication.sampleCommittedMetadataOffset(30);
        heartbeat(ctx, 0, 10);
        heartbeat(ctx, 1, 30);
        assertEquals(Set.of(), replication.softFencedBrokers());
        // Now it was committed 6 seconds ago, which is more than the maximum lag.
        ctx.time.sleep(3000);
        heartbeat(ctx, 0, 10);
        heartbeat(ctx, 1, 30);
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // The broker keeps the leadership it has, but does not get new leadership while another
        // replica is valid.
        assertEquals(OptionalInt.of(0), ctx.currentLeader(foo0));

        // The soft fence stays while the metadata lag is over the maximum, although the broker is
        // in sync for all of its partitions.
        assertEquals(0, replication.outOfSyncReplicaCount(0));
        ctx.time.sleep(2000);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(0), replication.softFencedBrokers());

        // Once the broker has caught up with the metadata log, the soft fence is lifted after
        // the stable time.
        heartbeat(ctx, 0, 30);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(0), replication.softFencedBrokers());
        ctx.time.sleep(1000);
        replication.maybeLiftSoftFences();
        assertEquals(Set.of(), replication.softFencedBrokers());
    }

    @Test
    public void testMetadataLagCheckIsDisabledByDefault() {
        ReplicationControlTestContext ctx = softFencingContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        replication.sampleCommittedMetadataOffset(10);
        ctx.time.sleep(8000);
        replication.sampleCommittedMetadataOffset(20);
        ctx.time.sleep(8000);
        heartbeat(ctx, 0, 5);
        heartbeat(ctx, 1, 20);
        assertEquals(Set.of(), replication.softFencedBrokers());
    }
}
