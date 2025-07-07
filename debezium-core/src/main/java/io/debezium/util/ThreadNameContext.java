package io.debezium.util;

import io.debezium.config.CommonConnectorConfig;

public class ThreadNameContext {
    private final String connectorName;
    private final String threadNamePattern;
    private final String taskId;

    /**
     * Creates a ThreadNameContext from CommonConnectorConfig
     *
     * @param connectorConfig the connector configuration
     * @return a new ThreadNameContext instance
     */
    public static ThreadNameContext from(CommonConnectorConfig connectorConfig) {
        return new ThreadNameContext(
                connectorConfig.connectorName(),
                connectorConfig.getConnectorThreadNamePattern(),
                connectorConfig.getTaskId());
    }

    /**
     * Creates a ThreadNameContext with the specified parameters
     *
     * @param connectorName the name of the connector
     * @param threadNamePattern the thread name pattern
     * @param taskId the task ID
     */
    public ThreadNameContext(String connectorName, String threadNamePattern, String taskId) {
        this.connectorName = connectorName;
        this.threadNamePattern = threadNamePattern;
        this.taskId = taskId;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getThreadNamePattern() {
        return threadNamePattern;
    }

    public String getTaskId() {
        return taskId;
    }
}
