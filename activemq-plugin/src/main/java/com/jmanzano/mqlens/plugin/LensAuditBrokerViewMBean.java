package com.jmanzano.mqlens.plugin;

public interface LensAuditBrokerViewMBean {
    long getAuditFailures();
    void resetAuditFailures();
}
