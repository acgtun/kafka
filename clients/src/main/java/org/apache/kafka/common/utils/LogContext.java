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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;

import java.util.Map;

/**
 * @deprecated Use {@link org.apache.kafka.common.utils.internals.LogContext} instead.
 * This class exists solely for source-level backward compatibility for code that
 * directly imported the old package location before the class was moved under
 * the {@code internals} package for better encapsulation.
 */
@Deprecated
public class LogContext extends org.apache.kafka.common.utils.internals.LogContext {

    public LogContext(String logPrefix) {
        super(logPrefix);
    }

    public LogContext(String logPrefix, Map<String, String> contextMap) {
        super(logPrefix, contextMap);
    }

    public LogContext() {
        super();
    }

    @Override
    public Logger logger(Class<?> clazz) {
        return super.logger(clazz);
    }

    @Override
    public Logger logger(String clazz) {
        return super.logger(clazz);
    }

    @Override
    public String logPrefix() {
        return super.logPrefix();
    }

    @Override
    public Map<String, String> contextMap() {
        return super.contextMap();
    }

    // The inner logger wrapper classes and MDC logic live in the internals implementation.
}