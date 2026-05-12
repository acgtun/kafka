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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 *
 * <p>In addition to the string prefix (which is prepended to every log message for human-readable
 * output), a structured context map can be provided. When present, the context map entries are
 * pushed to the SLF4J MDC before each log call, making them available as first-class fields in
 * structured logging layouts (e.g., JSON). This enables machine-parseable log output for AI-driven
 * debugging and log analysis tools.
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
     * @param contextMap  structured key-value pairs for MDC (may be empty but not null)
     */
    public LogContext(String logPrefix, Map<String, String> contextMap) {
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

        protected AbstractKafkaLogger(final String prefix, final Map<String, String> contextMap) {
            this.prefix = prefix;
            this.contextMap = contextMap;
        }

        protected String addPrefix(final String message) {
            return prefix + message;
        }

        /**
         * Push context map entries to MDC. Returns true if entries were pushed (and must be popped).
         */
        protected boolean pushMdc() {
            if (contextMap.isEmpty()) {
                return false;
            }
            for (Map.Entry<String, String> entry : contextMap.entrySet()) {
                MDC.put(entry.getKey(), entry.getValue());
            }
            return true;
        }

        /**
         * Remove context map entries from MDC.
         */
        protected void popMdc() {
            for (String key : contextMap.keySet()) {
                MDC.remove(key);
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

        @Override
        public void warn(String message) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, null, null);
        }

        @Override
        public void warn(String format, Object arg) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.WARN_INT, message, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(String format, Object... args) {
            writeLog(null, LocationAwareLogger.WARN_INT, format, args, null);
        }

        @Override
        public void warn(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void warn(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.WARN_INT, format, arguments, null);
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.WARN_INT, msg, null, t);
        }

        @Override
        public void error(String message) {
            writeLog(null, LocationAwareLogger.ERROR_INT, message, null, null);
        }

        @Override
        public void error(String format, Object arg) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(String format, Object... args) {
            writeLog(null, LocationAwareLogger.ERROR_INT, format, args, null);
        }

        @Override
        public void error(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void error(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, format, arguments, null);
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.ERROR_INT, msg, null, t);
        }

        @Override
        public void info(String msg) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(String format, Object arg) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(String format, Object... args) {
            writeLog(null, LocationAwareLogger.INFO_INT, format, args, null);
        }

        @Override
        public void info(String msg, Throwable t) {
            writeLog(null, LocationAwareLogger.INFO_INT, msg, null, t);
        }

        @Override
        public void info(Marker marker, String msg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            writeLog(marker, LocationAwareLogger.INFO_INT, format, arguments, null);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            writeLog(marker, LocationAwareLogger.INFO_INT, msg, null, t);
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
            boolean pushed = pushMdc();
            try {
                logger.log(marker, fqcn, level, addPrefix(message), null, exception);
            } finally {
                if (pushed) {
                    popMdc();
                }
            }
        }
    }

    private static class LocationIgnorantKafkaLogger extends AbstractKafkaLogger {
        private final Logger logger;

        LocationIgnorantKafkaLogger(String logPrefix, Map<String, String> contextMap, Logger logger) {
            super(logPrefix, contextMap);
            this.logger = logger;
        }

        private void logWithMdc(Runnable logAction) {
            boolean pushed = pushMdc();
            try {
                logAction.run();
            } finally {
                if (pushed) {
                    popMdc();
                }
            }
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

        @Override
        public void trace(String message) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(addPrefix(message)));
            }
        }

        @Override
        public void trace(String message, Object arg) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(addPrefix(message), arg));
            }
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(addPrefix(message), arg1, arg2));
            }
        }

        @Override
        public void trace(String message, Object... args) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(addPrefix(message), args));
            }
        }

        @Override
        public void trace(String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(addPrefix(msg), t));
            }
        }

        @Override
        public void trace(Marker marker, String msg) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(marker, addPrefix(msg)));
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(marker, addPrefix(format), arg));
            }
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(marker, addPrefix(format), arg1, arg2));
            }
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(marker, addPrefix(format), argArray));
            }
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            if (logger.isTraceEnabled()) {
                logWithMdc(() -> logger.trace(marker, addPrefix(msg), t));
            }
        }

        @Override
        public void debug(String message) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(addPrefix(message)));
            }
        }

        @Override
        public void debug(String message, Object arg) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(addPrefix(message), arg));
            }
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(addPrefix(message), arg1, arg2));
            }
        }

        @Override
        public void debug(String message, Object... args) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(addPrefix(message), args));
            }
        }

        @Override
        public void debug(String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(addPrefix(msg), t));
            }
        }

        @Override
        public void debug(Marker marker, String msg) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(marker, addPrefix(msg)));
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(marker, addPrefix(format), arg));
            }
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(marker, addPrefix(format), arg1, arg2));
            }
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(marker, addPrefix(format), arguments));
            }
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            if (logger.isDebugEnabled()) {
                logWithMdc(() -> logger.debug(marker, addPrefix(msg), t));
            }
        }

        @Override
        public void warn(String message) {
            logWithMdc(() -> logger.warn(addPrefix(message)));
        }

        @Override
        public void warn(String message, Object arg) {
            logWithMdc(() -> logger.warn(addPrefix(message), arg));
        }

        @Override
        public void warn(String message, Object arg1, Object arg2) {
            logWithMdc(() -> logger.warn(addPrefix(message), arg1, arg2));
        }

        @Override
        public void warn(String message, Object... args) {
            logWithMdc(() -> logger.warn(addPrefix(message), args));
        }

        @Override
        public void warn(String msg, Throwable t) {
            logWithMdc(() -> logger.warn(addPrefix(msg), t));
        }

        @Override
        public void warn(Marker marker, String msg) {
            logWithMdc(() -> logger.warn(marker, addPrefix(msg)));
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            logWithMdc(() -> logger.warn(marker, addPrefix(format), arg));
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            logWithMdc(() -> logger.warn(marker, addPrefix(format), arg1, arg2));
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            logWithMdc(() -> logger.warn(marker, addPrefix(format), arguments));
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            logWithMdc(() -> logger.warn(marker, addPrefix(msg), t));
        }

        @Override
        public void error(String message) {
            logWithMdc(() -> logger.error(addPrefix(message)));
        }

        @Override
        public void error(String message, Object arg) {
            logWithMdc(() -> logger.error(addPrefix(message), arg));
        }

        @Override
        public void error(String message, Object arg1, Object arg2) {
            logWithMdc(() -> logger.error(addPrefix(message), arg1, arg2));
        }

        @Override
        public void error(String message, Object... args) {
            logWithMdc(() -> logger.error(addPrefix(message), args));
        }

        @Override
        public void error(String msg, Throwable t) {
            logWithMdc(() -> logger.error(addPrefix(msg), t));
        }

        @Override
        public void error(Marker marker, String msg) {
            logWithMdc(() -> logger.error(marker, addPrefix(msg)));
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            logWithMdc(() -> logger.error(marker, addPrefix(format), arg));
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            logWithMdc(() -> logger.error(marker, addPrefix(format), arg1, arg2));
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            logWithMdc(() -> logger.error(marker, addPrefix(format), arguments));
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            logWithMdc(() -> logger.error(marker, addPrefix(msg), t));
        }

        @Override
        public void info(String message) {
            logWithMdc(() -> logger.info(addPrefix(message)));
        }

        @Override
        public void info(String message, Object arg) {
            logWithMdc(() -> logger.info(addPrefix(message), arg));
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            logWithMdc(() -> logger.info(addPrefix(message), arg1, arg2));
        }

        @Override
        public void info(String message, Object... args) {
            logWithMdc(() -> logger.info(addPrefix(message), args));
        }

        @Override
        public void info(String msg, Throwable t) {
            logWithMdc(() -> logger.info(addPrefix(msg), t));
        }

        @Override
        public void info(Marker marker, String msg) {
            logWithMdc(() -> logger.info(marker, addPrefix(msg)));
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            logWithMdc(() -> logger.info(marker, addPrefix(format), arg));
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            logWithMdc(() -> logger.info(marker, addPrefix(format), arg1, arg2));
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            logWithMdc(() -> logger.info(marker, addPrefix(format), arguments));
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            logWithMdc(() -> logger.info(marker, addPrefix(msg), t));
        }

    }

}
