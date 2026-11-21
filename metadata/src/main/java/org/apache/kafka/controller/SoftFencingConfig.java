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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.SoftFencingConfigs;

import java.util.function.Function;

/**
 * The soft fencing configuration that the active controller is currently using.
 *
 * @param enabled                   Whether soft fencing is enabled.
 * @param exitOutOfSyncPartitions   A broker is soft-fenced while it is out of sync for more partitions than this.
 * @param exitStableMs              How long the exit condition must hold before the soft fence is lifted.
 * @param maxMetadataLagMs          The metadata lag above which a broker is soft-fenced, or -1 to disable.
 * @param maxDurationMs             The longest time a broker stays soft-fenced, or 0 for no limit.
 */
public record SoftFencingConfig(
    boolean enabled,
    int exitOutOfSyncPartitions,
    long exitStableMs,
    long maxMetadataLagMs,
    long maxDurationMs
) {
    public static final SoftFencingConfig DEFAULT = fromConfigs(name -> null);

    /**
     * Resolve the configuration.
     *
     * @param lookup    Returns the configured value for a config name, or null if it is not set.
     *                  A value that is not valid is ignored and the default is used.
     */
    public static SoftFencingConfig fromConfigs(Function<String, Object> lookup) {
        return new SoftFencingConfig(
            (Boolean) resolve(SoftFencingConfigs.SOFT_FENCING_ENABLE_CONFIG, lookup),
            (Integer) resolve(SoftFencingConfigs.SOFT_FENCING_EXIT_OUT_OF_SYNC_PARTITIONS_CONFIG, lookup),
            (Long) resolve(SoftFencingConfigs.SOFT_FENCING_EXIT_STABLE_MS_CONFIG, lookup),
            (Long) resolve(SoftFencingConfigs.SOFT_FENCING_MAX_METADATA_LAG_MS_CONFIG, lookup),
            (Long) resolve(SoftFencingConfigs.SOFT_FENCING_MAX_DURATION_MS_CONFIG, lookup));
    }

    private static Object resolve(String name, Function<String, Object> lookup) {
        ConfigDef.ConfigKey key = SoftFencingConfigs.CONFIG_DEF.configKeys().get(name);
        Object value = lookup.apply(name);
        if (value != null) {
            try {
                Object parsed = ConfigDef.parseType(name, value, key.type);
                if (key.validator != null) {
                    key.validator.ensureValid(name, parsed);
                }
                return parsed;
            } catch (ConfigException e) {
                // Fall through to the default.
            }
        }
        return key.defaultValue;
    }
}
