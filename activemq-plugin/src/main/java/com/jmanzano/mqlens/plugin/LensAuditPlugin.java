package com.jmanzano.mqlens.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

public class LensAuditPlugin implements BrokerPlugin {

    private String auditDestination = "LENS.AUDIT.ALL";
    private String auditPrefix = "LENS.AUDIT.";
    private boolean enabled = true;
    private boolean failOnAuditError = false;
    private long maxAuditMessageSize = 10485760; // 10 MB default

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        if (!enabled) {
            return broker;
        }
        return new LensAuditBroker(broker, auditDestination, auditPrefix, failOnAuditError, maxAuditMessageSize);
    }

    public String getAuditDestination() {
        return auditDestination;
    }

    public void setAuditDestination(String auditDestination) {
        this.auditDestination = auditDestination;
    }

    public String getAuditPrefix() {
        return auditPrefix;
    }

    public void setAuditPrefix(String auditPrefix) {
        this.auditPrefix = auditPrefix;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isFailOnAuditError() {
        return failOnAuditError;
    }

    public void setFailOnAuditError(boolean failOnAuditError) {
        this.failOnAuditError = failOnAuditError;
    }

    public long getMaxAuditMessageSize() {
        return maxAuditMessageSize;
    }

    public void setMaxAuditMessageSize(long maxAuditMessageSize) {
        this.maxAuditMessageSize = maxAuditMessageSize;
    }
}
