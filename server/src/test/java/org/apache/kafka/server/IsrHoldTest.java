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
package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.SoftFencingConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, value = "true"),
        // Keep the ISR hold and the soft fence until the test lifts them.
        @ClusterConfigProperty(key = SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, value = "3600000"),
        @ClusterConfigProperty(key = ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG, value = "1")
    }
)
public class IsrHoldTest {
    private static final String TOPIC = "isr-hold-test";
    private static final String ISR_HOLD_BROKER_COUNT =
        "kafka.controller:type=KafkaController,name=IsrHoldBrokerCount";
    private static final String SOFT_FENCED_BROKER_COUNT =
        "kafka.controller:type=KafkaController,name=SoftFencedBrokerCount";

    private final ClusterInstance cluster;

    public IsrHoldTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.SOFT_FENCING_VERSION, version = 1)})
    public void testRestartedBrokerStaysOutOfIsrUntilHoldIsReleased() throws Exception {
        try (Admin admin = cluster.admin()) {
            restartPreferredLeader(admin, 1);

            // Broker 0 has caught up, but it is not added to the ISR, and acks=all does not need it.
            for (int i = 0; i < 30; i++) {
                produce(1);
                assertEquals(1, gauge(ISR_HOLD_BROKER_COUNT));
                assertEquals(Set.of(1, 2), isr(admin));
                Thread.sleep(100);
            }
            assertEquals(0, isrHoldBypasses());

            // Shorten the stable time. The hold is released, broker 0 joins the ISR, the soft
            // fence is lifted and leadership moves back to broker 0.
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.BROKER, ""), List.of(
                new AlterConfigOp(new ConfigEntry(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "0"),
                    AlterConfigOp.OpType.SET)))).all().get();
            waitForCondition(() -> gauge(ISR_HOLD_BROKER_COUNT) == 0, "The ISR hold was not released");
            waitForCondition(() -> isr(admin).equals(Set.of(0, 1, 2)), "Broker 0 did not join the ISR");
            waitForCondition(() -> gauge(SOFT_FENCED_BROKER_COUNT) == 0, "The soft fence was not lifted");
            waitForCondition(() -> leaderId(admin) == 0, "Leadership did not move back to broker 0");
            assertEquals(0, isrHoldBypasses());
        }
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.SOFT_FENCING_VERSION, version = 1)})
    public void testBrokerOnIsrHoldJoinsIsrWhenIsrIsBelowMinimum() throws Exception {
        try (Admin admin = cluster.admin()) {
            restartPreferredLeader(admin, 2);

            // Without broker 2 the ISR is below min.insync.replicas, so the leader adds broker 0
            // even though it is still on hold.
            cluster.shutdownBroker(2);
            waitForCondition(() -> isr(admin).equals(Set.of(0, 1)), "Broker 0 did not join the ISR");
            assertEquals(1, gauge(ISR_HOLD_BROKER_COUNT));
            waitForCondition(() -> isrHoldBypasses() == 1, "The ISR hold bypass was not counted");
            produce(10);
        }
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.SOFT_FENCING_VERSION, version = 1)})
    public void testBrokerOnIsrHoldJoinsIsrWhenMinInSyncReplicasIsRaised() throws Exception {
        try (Admin admin = cluster.admin()) {
            restartPreferredLeader(admin, 2);
            assertEquals(0, isrHoldBypasses());

            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC), List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"),
                    AlterConfigOp.OpType.SET)))).all().get();
            waitForCondition(() -> isr(admin).equals(Set.of(0, 1, 2)), "Broker 0 did not join the ISR");
            assertEquals(1, gauge(ISR_HOLD_BROKER_COUNT));
            waitForCondition(() -> isrHoldBypasses() == 1, "The ISR hold bypass was not counted");
            produce(10);
        }
    }

    @ClusterTest(features = {@ClusterFeature(feature = Feature.SOFT_FENCING_VERSION, version = 0)})
    public void testBrokerIsNotPutOnIsrHoldWhenFeatureIsNotEnabled() throws Exception {
        try (Admin admin = cluster.admin()) {
            createTopicAndStopPreferredLeader(admin, 1);
            cluster.startBroker(0);
            waitForCondition(() -> isr(admin).equals(Set.of(0, 1, 2)), "Broker 0 did not rejoin the ISR");
            assertEquals(1, gauge(SOFT_FENCED_BROKER_COUNT));
            assertEquals(0, gauge(ISR_HOLD_BROKER_COUNT));
        }
    }

    /**
     * Create a partition whose preferred leader is broker 0, then restart broker 0 and wait until
     * it is on ISR hold. Broker 1 is the leader afterwards and the ISR is brokers 1 and 2.
     */
    private void restartPreferredLeader(Admin admin, int minInSyncReplicas) throws Exception {
        createTopicAndStopPreferredLeader(admin, minInSyncReplicas);
        cluster.startBroker(0);
        waitForCondition(() -> gauge(ISR_HOLD_BROKER_COUNT) == 1, "Broker 0 was not put on ISR hold");
        assertEquals(1, gauge(SOFT_FENCED_BROKER_COUNT));
        assertEquals(Set.of(1, 2), isr(admin));
    }

    private void createTopicAndStopPreferredLeader(Admin admin, int minInSyncReplicas) throws Exception {
        admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0, 1, 2))).configs(
            Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minInSyncReplicas))))).all().get();
        cluster.waitTopicCreation(TOPIC, 1);
        waitForCondition(() -> leaderId(admin) == 0, "Broker 0 did not become the leader");
        assertEquals(0, gauge(ISR_HOLD_BROKER_COUNT));
        produce(10);

        cluster.shutdownBroker(0);
        waitForCondition(() -> leaderId(admin) == 1, "Broker 1 did not become the leader");
        produce(10);
    }

    private void produce(int records) throws Exception {
        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of(ProducerConfig.ACKS_CONFIG, "all"))) {
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>(TOPIC, 0, null, new byte[100])).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static TopicPartitionInfo partition(Admin admin) throws Exception {
        TopicDescription description = admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC);
        return description.partitions().get(0);
    }

    private static int leaderId(Admin admin) throws Exception {
        Node leader = partition(admin).leader();
        return leader == null ? -1 : leader.id();
    }

    private static Set<Integer> isr(Admin admin) throws Exception {
        return partition(admin).isr().stream().map(Node::id).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static int gauge(String mbeanName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream().
            filter(entry -> entry.getKey().getMBeanName().equals(mbeanName)).
            map(entry -> ((Gauge<Integer>) entry.getValue()).value()).
            findFirst().
            orElseThrow(() -> new AssertionError("No metric " + mbeanName));
    }

    /**
     * Read the meter of broker 1, the leader, from the broker itself. The brokers share one
     * metrics registry, and the meter is not in it any more once any broker has shut down.
     */
    private long isrHoldBypasses() {
        return cluster.brokers().get(1).replicaManager().isrHoldBypassRate().count();
    }
}
