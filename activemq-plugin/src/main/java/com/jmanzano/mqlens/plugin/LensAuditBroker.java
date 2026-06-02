package com.jmanzano.mqlens.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.concurrent.atomic.AtomicLong;

public class LensAuditBroker extends BrokerFilter implements LensAuditBrokerViewMBean {

    private static final Logger LOG = LoggerFactory.getLogger(LensAuditBroker.class);
    private final ActiveMQDestination auditDestination;
    private final String auditDestinationName;
    private final String auditPrefix;
    private final boolean failOnAuditError;
    private final long maxAuditMessageSize;
    private final AtomicLong auditFailures = new AtomicLong(0);
    private final ThreadLocal<ProducerBrokerExchange> systemExchangeLocal = new ThreadLocal<>();
    private final ProducerState auditProducerState;

    public LensAuditBroker(Broker next, String auditDestinationName, String auditPrefix, boolean failOnAuditError, long maxAuditMessageSize) {
        super(next);
        this.auditDestinationName = auditDestinationName;
        this.auditPrefix = auditPrefix;
        this.failOnAuditError = failOnAuditError;
        this.maxAuditMessageSize = maxAuditMessageSize;

        // Initialize a stable internal producer state for audit forwarding
        ProducerId producerId = new ProducerId();
        producerId.setConnectionId(new IdGenerator("LensAuditPlugin").generateId());
        producerId.setSessionId(-1);
        producerId.setValue(1);
        ProducerInfo producerInfo = new ProducerInfo(producerId);
        this.auditProducerState = new ProducerState(producerInfo);

        this.auditDestination = new ActiveMQQueue(auditDestinationName);

        try {
            ObjectName objectName = new ObjectName(
                next.getBrokerService().getBrokerObjectName().toString() + ",plugin=LensAuditBroker"
            );
            AnnotatedMBean.registerMBean(next.getBrokerService().getManagementContext(), this, objectName);
            LOG.info("Registered LensAuditBroker MBean: {}", objectName);
        } catch (Exception e) {
            LOG.warn("Failed to register LensAuditBroker JMX MBean", e);
        }

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
            MessageId oldId = messageSend.getMessageId();
            MessageId newId = new MessageId(oldId.getProducerId(), oldId.getProducerSequenceId() + 1000000000L);
            auditMessage.setMessageId(newId);
            auditMessage.setProperty("LENS_OriginalMessageId", oldId.toString());

            // 4. Send the copy to the audit destination using a synthetic exchange to avoid transaction/metrics pollution
            ProducerBrokerExchange auditExchange = systemExchangeLocal.get();
            if (auditExchange == null) {
                auditExchange = new ProducerBrokerExchange();
                auditExchange.setMutable(true);
                auditExchange.setProducerState(auditProducerState);
                systemExchangeLocal.set(auditExchange);
            }
            auditExchange.setConnectionContext(producerExchange.getConnectionContext());

            try {
                super.send(auditExchange, auditMessage);
            } finally {
                systemExchangeLocal.remove();
            }
            
            LOG.debug("Audited message to {}", auditDestinationName);
        } catch (Exception e) {
            auditFailures.incrementAndGet();
            LOG.error("Failed to audit message", e);
            if (failOnAuditError) {
                throw new RuntimeException("Audit copy failed", e);
            }
        }
    }

    @Override
    public long getAuditFailures() {
        return auditFailures.get();
    }

    @Override
    public void resetAuditFailures() {
        auditFailures.set(0);
    }
}
