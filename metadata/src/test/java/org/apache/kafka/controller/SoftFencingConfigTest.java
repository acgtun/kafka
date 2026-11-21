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

import org.apache.kafka.server.config.SoftFencingConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SoftFencingConfigTest {
    @Test
    public void testDefaults() {
        assertEquals(new SoftFencingConfig(false, 0, 60000L, -1L, 0L, false, 1), SoftFencingConfig.DEFAULT);
    }

    @Test
    public void testFromConfigsParsesStringsAndTypedValues() {
        Map<String, Object> configs = Map.of(
            SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "true",
            SoftFencingConfigs.SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG, 5,
            SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "1000",
            SoftFencingConfigs.SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG, 30000L,
            SoftFencingConfigs.SOFT_FENCING_MAX_DURATION_MS_CONFIG, "3600000",
            SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, "true",
            SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG, "3");
        assertEquals(new SoftFencingConfig(true, 5, 1000L, 30000L, 3600000L, true, 3),
            SoftFencingConfig.fromConfigs(configs::get));
    }

    @Test
    public void testInvalidValuesFallBackToDefaults() {
        Map<String, Object> configs = Map.of(
            SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, "maybe",
            SoftFencingConfigs.SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG, "-1",
            SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, "soon",
            SoftFencingConfigs.SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG, "-2",
            SoftFencingConfigs.SOFT_FENCING_MAX_DURATION_MS_CONFIG, "7200000",
            SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_ENABLE_CONFIG, "yes",
            SoftFencingConfigs.SOFT_FENCING_ISR_HOLD_MAX_BROKERS_CONFIG, "0");
        assertEquals(new SoftFencingConfig(false, 0, 60000L, -1L, 7200000L, false, 1),
            SoftFencingConfig.fromConfigs(configs::get));
    }
}
