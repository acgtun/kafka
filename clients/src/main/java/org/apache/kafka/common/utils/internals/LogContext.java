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
package org.apache.kafka.common.utils.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 *
 * <p>In addition to the string prefix (which is prepended to every log message for human-readable
 * output), a structured context map can be provided. When present, the context map entries are
 * pushed to the SLF4J MDC before each log call and restored after, making them available as
 * first-class fields in structured logging layouts (e.g., JSON). This enables machine-parseable
 * log output for AI-driven debugging and log analysis tools.
 *
 * <p><b>Thread safety:</b> MDC is backed by a ThreadLocal, so concurrent threads using the same
 * {@code LogContext} instance will not interfere with each other. Pre-existing MDC values are
 * saved before each log call and restored afterwards.
 *
 * <p><b>Async logging:</b> Log4j2's default {@code AsyncAppender} snapshots MDC at enqueue time,
 * so structured context is preserved. Custom async implementations that do not snapshot MDC may
 * lose context fields.
 */
public class LogContext {

    private final String logPrefix;
    private final Map<String, String> contextMap;

    public LogContext(String logPrefix) {
        this(logPrefix, Collections.emptyMap());
    }

    /**
     * Create a LogContext with both a human-readable prefix and a structured context map.
     * The prefix is prepended to every log message (backward-compatible behavior).
     * The context map entries are pushed to SLF4J MDC on each log call, making them available
     * as structured fields in JSON logging layouts.
     *
     * @param logPrefix   the string prefix for log messages (may be null or empty)
     * @param contextMap  structured key-value pairs for MDC (must not be null; may be empty)
     */
    public LogContext(String logPrefix, Map<String, String> contextMap) {
        Objects.requireNonNull(contextMap, "contextMap must not be null");
        this.logPrefix = logPrefix == null ? "" : logPrefix;
        this.contextMap = contextMap.isEmpty()
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(contextMap));
    }

    public LogContext() {
        this("", Collections.emptyMap());
    }

    public Logger logger(Class<?> clazz) {
        return logger(LoggerFactory.getLogger(clazz));
    }

    public Logger logger(String clazz) {
        return logger(LoggerFactory.getLogger(clazz));
    }

    private Logger logger(Logger logger) {
        if (logger instanceof LocationAwareLogger) {
            return new LocationAwareKafkaLogger(logPrefix, contextMap, (LocationAwareLogger) logger);
        } else {
            return new LocationIgnorantKafkaLogger(logPrefix, contextMap, logger);
        }
    }

    public String logPrefix() {
        return logPrefix;
    }

    /**
     * Returns the structured context map. When non-empty, these entries are pushed to
     * SLF4J MDC on each log call.
     */
    public Map<String, String> contextMap() {
        return contextMap;
    }

    private abstract static class AbstractKafkaLogger implements Logger {
        private final String prefix;
        private final Map<String, String> contextMap;
        private final boolean hasContext;

        protected AbstractKafkaLogger(final String prefix, final Map<String, String> contextMap) {
            this.prefix = prefix;
            this.contextMap = contextMap;
            this.hasContext = !contextMap.isEmpty();
        }

        protected String addPrefix(final String message) {
            return prefix + message;
        }

        /**
         * Push context map entries to MDC, saving any pre-existing values so they can be
         * restored by {@link #popMdc(Map)}. Returns the saved values, or {@code null} if
         * there is no context to push.
         *
         * <p>This save/restore pattern ensures that pre-existing MDC entries (e.g., from
         * Kafka Connect's {@code LoggingContext} or user-set values) are not clobbered.
         */
        protected Map<String, String> pushMdc() {
            if (!hasContext) {
                return null;
            }
            Map<String, String> saved = new HashMap<>(contextMap.size());
            for (Map.Entry<String, String> entry : contextMap.entrySet()) {
                saved.put(entry.getKey(), MDC.get(entry.getKey()));
                MDC.put(entry.getKey(), entry.getValue());
            }
            return saved;
        }

        /**
         * Restore MDC to the state captured by {@link #pushMdc()}.
         *
         * @param saved the saved MDC values returned by {@link #pushMdc()}; may be {@code null}
         */
        protected void popMdc(Map<String, String> saved) {
            if (saved == null) {
                return;
            }
            for (Map.Entry<String, String> entry : saved.entrySet()) {
                if (entry.getValue() == null) {
                    MDC.remove(entry.getKey());
                } else {
                    MDC.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static class LocationAwareKafkaLogger extends AbstractKafkaLogger {
        private final LocationAwareLogger logger;
        private final String fqcn;

        LocationAwareKafkaLogger(String logPrefix, Map<String, String> contextMap, LocationAwareLogger logger) {
            super(logPrefix, contextMap);
            this.logger = logger;
            this.fqcn = LocationAwareKafkaLogger.class.getName();
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        // --- trace (guarded) ---

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, message, null, null);
            }
        }

        @Override
        public void trace(String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(String format, Object... args) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, format, args, null);
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(null, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, format, argArray, null);
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                writeLog(marker, LocationAwareLogger.TRACE_INT, msg, null, t);
            }
        }

        // --- debug (guarded) ---

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, message, null, null);
            }
        }

        @Override
        public void debug(String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(String format, Object... args) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, format, args, null);
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(null, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, format, arguments, null);
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                writeLog(marker, LocationAwareLogger.DEBUG_INT, msg, null, t);
            }
        }

        // --- info (now guarded — M-2 fix) ---

        @Override
        public void info(String msg) {
            if (logger.isInfoEnabled()) {
                writeLog(null, LocationAwareLogger.INFO_INT, msg, null, null);
            }
        }

        @Override
        public void info(String format, Object arg) {
            if (logger.isInfoEnabled()) {
                writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            if (logger.isInfoEnabled()) {
                writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void info(String format, Object... args) {
            if (logger.isInfoEnabled()) {
                writeLog(null, LocationAwareLogger.INFO_INT, format, args, null);
            }
        }

        @Override
        public void info(String msg, Throwable t) {
            if (logger.isInfoEnabled()) {
                writeLog(null, LocationAwareLogger.INFO_INT, msg, null, t);
            }
        }

        @Override
        public void info(Marker marker, String msg) {
            if (logger.isInfoEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, null);
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            if (logger.isInfoEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isInfoEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            if (logger.isInfoEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.INFO_INT, format, arguments, null);
            }
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            if (logger.isInfoEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, t);
            }
        }

        // --- warn (now guarded — M-2 fix) ---

        @Override
        public void warn(String message) {
            if (logger.isWarnEnabled()) {
                writeLog(null, LocationAwareLogger.WARN_INT, message, null, null);
            }
        }

        @Override
        public void warn(String format, Object arg) {
            if (logger.isWarnEnabled()) {
                writeLog(null, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            if (logger.isWarnEnabled()) {
                writeLog(null, LocationAwareLogger.WARN_INT, message, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void warn(String format, Object... args) {
            if (logger.isWarnEnabled()) {
                writeLog(null, LocationAwareLogger.WARN_INT, format, args, null);
            }
        }

        @Override
        public void warn(String msg, Throwable t) {
            if (logger.isWarnEnabled()) {
                writeLog(null, LocationAwareLogger.WARN_INT, msg, null, t);
            }
        }

        @Override
        public void warn(Marker marker, String msg) {
            if (logger.isWarnEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, null);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            if (logger.isWarnEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isWarnEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            if (logger.isWarnEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.WARN_INT, format, arguments, null);
            }
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            if (logger.isWarnEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, t);
            }
        }

        // --- error (now guarded — M-2 fix) ---

        @Override
        public void error(String message) {
            if (logger.isErrorEnabled()) {
                writeLog(null, LocationAwareLogger.ERROR_INT, message, null, null);
            }
        }

        @Override
        public void error(String format, Object arg) {
            if (logger.isErrorEnabled()) {
                writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            if (logger.isErrorEnabled()) {
                writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void error(String format, Object... args) {
            if (logger.isErrorEnabled()) {
                writeLog(null, LocationAwareLogger.ERROR_INT, format, args, null);
            }
        }

        @Override
        public void error(String msg, Throwable t) {
            if (logger.isErrorEnabled()) {
                writeLog(null, LocationAwareLogger.ERROR_INT, msg, null, t);
            }
        }

        @Override
        public void error(Marker marker, String msg) {
            if (logger.isErrorEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, null);
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            if (logger.isErrorEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isErrorEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
            }
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            if (logger.isErrorEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.ERROR_INT, format, arguments, null);
            }
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            if (logger.isErrorEnabled(marker)) {
                writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, t);
            }
        }

        private void writeLog(Marker marker, int level, String format, Object[] args, Throwable exception) {
            String message = format;
            if (args != null && args.length > 0) {
                FormattingTuple formatted = MessageFormatter.arrayFormat(format, args);
                if (exception == null && formatted.getThrowable() != null) {
                    exception = formatted.getThrowable();
                }
                message = formatted.getMessage();
            }
            Map<String, String> saved = pushMdc();
            try {
                logger.log(marker, fqcn, level, addPrefix(message), null, exception);
            } finally {
                popMdc(saved);
            }
        }
    }

    /**
     * Logger implementation for SLF4J backends that do not implement {@link LocationAwareLogger}.
     * MDC push/pop is inlined in each method (no lambda allocation) and all levels are guarded
     * to avoid unnecessary MDC operations when the level is disabled.
     */
    private static class LocationIgnorantKafkaLogger extends AbstractKafkaLogger {
        private final Logger logger;

        LocationIgnorantKafkaLogger(String logPrefix, Map<String, String> contextMap, Logger logger) {
            super(logPrefix, contextMap);
            this.logger = logger;
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return logger.isTraceEnabled(marker);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return logger.isDebugEnabled(marker);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return logger.isInfoEnabled(marker);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return logger.isWarnEnabled(marker);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return logger.isErrorEnabled(marker);
        }

        // --- trace ---

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(addPrefix(message));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(String message, Object arg) {
            if (logger.isTraceEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(addPrefix(message), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(addPrefix(message), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(String message, Object... args) {
            if (logger.isTraceEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(addPrefix(message), args);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(marker, addPrefix(msg));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(marker, addPrefix(format), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(marker, addPrefix(format), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(marker, addPrefix(format), argArray);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.trace(marker, addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        // --- debug ---

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(addPrefix(message));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(String message, Object arg) {
            if (logger.isDebugEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(addPrefix(message), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(addPrefix(message), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(String message, Object... args) {
            if (logger.isDebugEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(addPrefix(message), args);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(marker, addPrefix(msg));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(marker, addPrefix(format), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(marker, addPrefix(format), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(marker, addPrefix(format), arguments);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.debug(marker, addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        // --- info (guarded — M-3 fix) ---

        @Override
        public void info(String message) {
            if (logger.isInfoEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(addPrefix(message));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(String message, Object arg) {
            if (logger.isInfoEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(addPrefix(message), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            if (logger.isInfoEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(addPrefix(message), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(String message, Object... args) {
            if (logger.isInfoEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(addPrefix(message), args);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(String msg, Throwable t) {
            if (logger.isInfoEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(Marker marker, String msg) {
            if (logger.isInfoEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(marker, addPrefix(msg));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            if (logger.isInfoEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(marker, addPrefix(format), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isInfoEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(marker, addPrefix(format), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            if (logger.isInfoEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(marker, addPrefix(format), arguments);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            if (logger.isInfoEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.info(marker, addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        // --- warn (guarded — M-3 fix) ---

        @Override
        public void warn(String message) {
            if (logger.isWarnEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(addPrefix(message));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(String message, Object arg) {
            if (logger.isWarnEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(addPrefix(message), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            if (logger.isWarnEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(addPrefix(message), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(String message, Object... args) {
            if (logger.isWarnEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(addPrefix(message), args);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(String msg, Throwable t) {
            if (logger.isWarnEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(Marker marker, String msg) {
            if (logger.isWarnEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(marker, addPrefix(msg));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            if (logger.isWarnEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(marker, addPrefix(format), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isWarnEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(marker, addPrefix(format), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            if (logger.isWarnEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(marker, addPrefix(format), arguments);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            if (logger.isWarnEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.warn(marker, addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        // --- error (guarded — M-3 fix) ---

        @Override
        public void error(String message) {
            if (logger.isErrorEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(addPrefix(message));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(String message, Object arg) {
            if (logger.isErrorEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(addPrefix(message), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(String message, Object arg1, Object arg2) {
            if (logger.isErrorEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(addPrefix(message), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(String message, Object... args) {
            if (logger.isErrorEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(addPrefix(message), args);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(String msg, Throwable t) {
            if (logger.isErrorEnabled()) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(Marker marker, String msg) {
            if (logger.isErrorEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(marker, addPrefix(msg));
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            if (logger.isErrorEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(marker, addPrefix(format), arg);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isErrorEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(marker, addPrefix(format), arg1, arg2);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            if (logger.isErrorEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(marker, addPrefix(format), arguments);
                } finally {
                    popMdc(saved);
                }
            }
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            if (logger.isErrorEnabled(marker)) {
                Map<String, String> saved = pushMdc();
                try {
                    logger.error(marker, addPrefix(msg), t);
                } finally {
                    popMdc(saved);
                }
            }
        }

    }

}
