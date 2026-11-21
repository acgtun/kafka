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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

/**
 * Configs for soft fencing. A soft-fenced broker is unfenced, but the controller elects it as a
 * partition leader only when no other replica can be elected. These configs are read by the active
 * controller and can be changed dynamically for the whole cluster.
 */
public final class SoftFencingConfigs {
    public static final String SOFT_FENCING_ENABLE_CONFIG = "soft.fencing.enable";
    public static final boolean SOFT_FENCING_ENABLE_DEFAULT = false;
    public static final String SOFT_FENCING_ENABLE_DOC = "Enables soft fencing. When the controller unfences a broker that " +
        "is still out of sync for its partitions, it soft-fences the broker: the broker is elected as a partition leader " +
        "only when no other replica can be elected, until it has caught up.";

    public static final String SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG = "soft.fencing.exit.out.of.sync.partitions";
    public static final int SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_DEFAULT = 0;
    public static final String SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_DOC = "A broker is soft-fenced while it is out of the " +
        "ISR of more than this number of its partitions. Partitions that are being reassigned onto the broker and " +
        "partitions that have no leader are not counted.";

    public static final String SOFT_FENCING_EXIT_STABLE_MS_CONFIG = "soft.fencing.exit.stable.ms";
    public static final long SOFT_FENCING_EXIT_STABLE_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
    public static final String SOFT_FENCING_EXIT_STABLE_MS_DOC = "How long a soft-fenced broker must continuously meet the " +
        "exit condition before the controller lifts the soft fence.";

    public static final String SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG = "soft.fencing.max.metadata.lag.ms";
    public static final long SOFT_FENCING_MAX_METADATA_LAG_MS_DEFAULT = -1L;
    public static final String SOFT_FENCING_MAX_METADATA_LAG_MS_DOC = "The controller soft-fences an unfenced broker when the " +
        "oldest metadata record the broker has not applied yet was committed more than this long ago. The lag is " +
        "measured from broker heartbeats, so values below the heartbeat interval are not useful. -1 disables the check.";

    public static final String SOFT_FENCING_MAX_DURATION_MS_CONFIG = "soft.fencing.max.duration.ms";
    public static final long SOFT_FENCING_MAX_DURATION_MS_DEFAULT = 0L;
    public static final String SOFT_FENCING_MAX_DURATION_MS_DOC = "The longest time a broker stays soft-fenced. After this " +
        "time the controller lifts the soft fence even if the broker has not caught up. 0 means no limit.";

    public static final String SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG = "soft.fencing.isr.hold.enable";
    public static final boolean SOFT_FENCING_ISR_HOLD_ENABLE_DEFAULT = false;
    public static final String SOFT_FENCING_ISR_HOLD_ENABLE_DOC = "Enables ISR hold. When the controller soft-fences a broker " +
        "that it unfences, it also puts the broker on ISR hold: partition leaders do not add the broker to the ISR, " +
        "unless the ISR is below <code>min.insync.replicas</code>, until the broker reports that it has caught up. " +
        "This requires <code>" + SOFT_FENCING_ENABLE_CONFIG + "</code> and the feature <code>soft.fencing.version</code> " +
        "at level 1 or higher.";

    public static final String SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG = "soft.fencing.isr.hold.max.brokers";
    public static final int SOFT_FENCING_ISR_HOLD_MAX_BROKERS_DEFAULT = 1;
    public static final String SOFT_FENCING_ISR_HOLD_MAX_BROKERS_DOC = "The largest number of brokers that can be on ISR hold " +
        "at the same time. A broker that is unfenced while this many brokers are on hold is only soft-fenced.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(SOFT_FENCING_ENABLE_CONFIG, BOOLEAN, SOFT_FENCING_ENABLE_DEFAULT, MEDIUM, SOFT_FENCING_ENABLE_DOC)
        .define(SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG, INT, SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_DEFAULT, atLeast(0), LOW, SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_DOC)
        .define(SOFT_FENCING_EXIT_STABLE_MS_CONFIG, LONG, SOFT_FENCING_EXIT_STABLE_MS_DEFAULT, atLeast(0), LOW, SOFT_FENCING_EXIT_STABLE_MS_DOC)
        .define(SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG, LONG, SOFT_FENCING_MAX_METADATA_LAG_MS_DEFAULT, atLeast(-1), LOW, SOFT_FENCING_MAX_METADATA_LAG_MS_DOC)
        .define(SOFT_FENCING_MAX_DURATION_MS_CONFIG, LONG, SOFT_FENCING_MAX_DURATION_MS_DEFAULT, atLeast(0), LOW, SOFT_FENCING_MAX_DURATION_MS_DOC)
        .define(SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, BOOLEAN, SOFT_FENCING_ISR_HOLD_ENABLE_DEFAULT, MEDIUM, SOFT_FENCING_ISR_HOLD_ENABLE_DOC)
        .define(SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG, INT, SOFT_FENCING_ISR_HOLD_MAX_BROKERS_DEFAULT, atLeast(1), LOW, SOFT_FENCING_ISR_HOLD_MAX_BROKERS_DOC);

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        SOFT_FENCING_ENABLE_CONFIG,
        SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG,
        SOFT_FENCING_EXIT_STABLE_MS_CONFIG,
        SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG,
        SOFT_FENCING_MAX_DURATION_MS_CONFIG,
        SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG,
        SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG);

    private SoftFencingConfigs() { }
}
