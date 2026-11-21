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
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.SoftFencingConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, value = "true"),
        // Keep the soft fence until the test lifts it.
        @ClusterConfigProperty(key = SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, value = "3600000"),
        @ClusterConfigProperty(key = ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG, value = "1")
    }
)
public class SoftFencingTest {
    private static final String TOPIC = "soft-fencing-test";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
    private static final String SOFT_FENCED_BROKER_COUNT =
        "kafka.controller:type=KafkaController,name=SoftFencedBrokerCount";

    private final ClusterInstance cluster;

    public SoftFencingTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testRestartedBrokerDoesNotLeadUntilSoftFenceIsLifted() throws Exception {
        try (Admin admin = cluster.admin()) {
            restartPreferredLeader(admin);

            // The broker is back in the ISR, but the preferred leader election that runs every
            // second does not move leadership back to it.
            for (int i = 0; i < 30; i++) {
                assertEquals(1, softFencedBrokerCount());
                assertEquals(1, partition(admin).leader().id());
                Thread.sleep(100);
            }

            // A requested preferred election does not either.
            Optional<Throwable> error = admin.electLeaders(ElectionType.PREFERRED, Set.of(TOPIC_PARTITION)).
                partitions().get().get(TOPIC_PARTITION);
            assertTrue(error.isPresent());
            assertInstanceOf(PreferredLeaderNotAvailableException.class, error.get());

            // Shorten the stable time. The soft fence is lifted and leadership moves back.
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.BROKER, ""), List.of(
                new AlterConfigOp(new ConfigEntry(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "0"),
                    AlterConfigOp.OpType.SET)))).all().get();
            waitForCondition(() -> softFencedBrokerCount() == 0, "The soft fence was not lifted");
            waitForCondition(() -> partition(admin).leader().id() == 0, "Leadership did not move back to broker 0");
        }
    }

    @ClusterTest
    public void testSoftFencedBrokerIsElectedWhenNoOtherReplicaIsAvailable() throws Exception {
        try (Admin admin = cluster.admin()) {
            restartPreferredLeader(admin);

            // Broker 0 is first in the replica list and in the ISR, but broker 2 is elected.
            cluster.shutdownBroker(1);
            waitForCondition(() -> leaderId(admin) == 2, "Broker 2 was not elected");
            assertEquals(1, softFencedBrokerCount());

            // With broker 2 gone as well, the soft-fenced broker is the only choice.
            cluster.shutdownBroker(2);
            waitForCondition(() -> leaderId(admin) == 0, "Broker 0 was not elected");
        }
    }

    /**
     * Create a partition whose preferred leader is broker 0, then restart broker 0 and wait until
     * it is soft-fenced and back in the ISR. Broker 1 is the leader afterwards.
     */
    private void restartPreferredLeader(Admin admin) throws Exception {
        admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0, 1, 2))))).all().get();
        cluster.waitTopicCreation(TOPIC, 1);
        waitForCondition(() -> leaderId(admin) == 0, "Broker 0 did not become the leader");
        assertEquals(0, softFencedBrokerCount());

        cluster.shutdownBroker(0);
        waitForCondition(() -> leaderId(admin) == 1, "Broker 1 did not become the leader");
        cluster.startBroker(0);
        waitForCondition(() -> softFencedBrokerCount() == 1, "Broker 0 was not soft-fenced");
        waitForCondition(() -> isr(admin).equals(Set.of(0, 1, 2)), "Broker 0 did not rejoin the ISR");
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
    private static int softFencedBrokerCount() {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream().
            filter(entry -> entry.getKey().getMBeanName().equals(SOFT_FENCED_BROKER_COUNT)).
            map(entry -> ((Gauge<Integer>) entry.getValue()).value()).
            findFirst().
            orElseThrow(() -> new AssertionError("No metric " + SOFT_FENCED_BROKER_COUNT));
    }
}
