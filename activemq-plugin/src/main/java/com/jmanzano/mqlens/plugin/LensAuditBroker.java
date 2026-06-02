package com.jmanzano.mqlens.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LensAuditBroker extends BrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LensAuditBroker.class);
    private final ActiveMQDestination auditDestination;
    private final String auditDestinationName;
    private final String auditPrefix;
    private final boolean failOnAuditError;
    private final long maxAuditMessageSize;
    private final java.util.concurrent.atomic.AtomicLong auditFailures = new java.util.concurrent.atomic.AtomicLong(0);
    private final ThreadLocal<ProducerBrokerExchange> systemExchangeLocal = new ThreadLocal<>();

    public LensAuditBroker(Broker next, String auditDestinationName, String auditPrefix, boolean failOnAuditError, long maxAuditMessageSize) {
        super(next);
        this.auditDestinationName = auditDestinationName;
        this.auditPrefix = auditPrefix;
        this.failOnAuditError = failOnAuditError;
        this.maxAuditMessageSize = maxAuditMessageSize;
        this.auditDestination = new ActiveMQQueue(auditDestinationName);
        LOG.info("LensAuditBroker initialized. Forwarding copies to: {}", auditDestinationName);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        // 1. Process original message
        super.send(producerExchange, messageSend);

        // 2. Determine if we should audit this message
        ActiveMQDestination originalDest = messageSend.getDestination();
        if (originalDest == null) {
            return;
        }

        String destName = originalDest.getPhysicalName();
        
        // Skip advisory topics, our own audit queue, and DLQs to avoid infinite loops
        if (destName.startsWith("ActiveMQ.Advisory") || 
            destName.startsWith(auditPrefix) || 
            destName.equals(auditDestinationName) ||
            destName.contains(".DLQ") || destName.startsWith("ActiveMQ.DLQ")) {
            return;
        }

        if (messageSend.getSize() > maxAuditMessageSize) {
            LOG.warn("Message too large to audit ({} bytes). Max allowed is {}", messageSend.getSize(), maxAuditMessageSize);
            return;
        }

        try {
            // 3. Create a copy of the message for the audit queue
            Message auditMessage = messageSend.copy();
            
            // Set the new destination
            auditMessage.setDestination(auditDestination);
            
            // Add custom property so MQ Lens knows where it came from
            auditMessage.setProperty("LENS_OriginalDestination", destName);
            auditMessage.setProperty("LENS_DestinationType", originalDest.isTopic() ? "Topic" : "Queue");

            // Avoid deduplication discards by generating a distinct message ID
            org.apache.activemq.command.MessageId newId = new org.apache.activemq.command.MessageId(messageSend.getMessageId().toString() + "-audit");
            auditMessage.setMessageId(newId);
            auditMessage.clearMarshalledState();

            // 4. Send the copy to the audit destination using a synthetic exchange to avoid transaction/metrics pollution
            ProducerBrokerExchange auditExchange = systemExchangeLocal.get();
            if (auditExchange == null) {
                auditExchange = new ProducerBrokerExchange();
                auditExchange.setMutable(true);
                systemExchangeLocal.set(auditExchange);
            }
            auditExchange.setConnectionContext(producerExchange.getConnectionContext());
            
            super.send(auditExchange, auditMessage);
            
            LOG.debug("Audited message to {}", auditDestinationName);
        } catch (Exception e) {
            auditFailures.incrementAndGet();
            LOG.error("Failed to audit message", e);
            if (failOnAuditError) {
                throw new RuntimeException("Audit copy failed", e);
            }
        }
    }

    public long getAuditFailures() {
        return auditFailures.get();
    }
}
