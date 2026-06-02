package com.jmanzano.mqlens.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class LensAuditBrokerTest {

    private Broker mockNext;
    private LensAuditBroker auditBroker;
    private ProducerBrokerExchange mockExchange;

    @BeforeEach
    public void setup() {
        mockNext = mock(Broker.class);
        auditBroker = new LensAuditBroker(mockNext, "LENS.AUDIT.ALL", "LENS.AUDIT.", true, 1024 * 1024);
        mockExchange = mock(ProducerBrokerExchange.class);
    }

    @Test
    public void testAuditCopyIsCreated() throws Exception {
        org.apache.activemq.command.ActiveMQTextMessage realMessage = new org.apache.activemq.command.ActiveMQTextMessage();
        realMessage.setDestination(new ActiveMQQueue("BUSINESS.QUEUE"));
        realMessage.setText("payload");
        realMessage.setProperty("custom-header", "value");
        realMessage.setMessageId(new MessageId("ID:test-1"));
        realMessage.setPersistent(true);
        realMessage.setExpiration(12345L);

        auditBroker.send(mockExchange, realMessage);

        // Verify original message is sent
        verify(mockNext).send(mockExchange, realMessage);

        // Verify audit message is sent
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockNext).send(any(ProducerBrokerExchange.class), messageCaptor.capture());

        org.apache.activemq.command.ActiveMQTextMessage captured = (org.apache.activemq.command.ActiveMQTextMessage) messageCaptor.getValue();
        
        assertEquals("BUSINESS.QUEUE", captured.getProperty("LENS_OriginalDestination"));
        assertEquals("Queue", captured.getProperty("LENS_DestinationType"));
        assertEquals("value", captured.getProperty("custom-header"));
        assertEquals("payload", captured.getText());
        assertTrue(captured.isPersistent());
        assertEquals(12345L, captured.getExpiration());
        assertTrue(captured.getMessageId().toString().endsWith("-audit"));
    }

    @Test
    public void testAntiLoop() throws Exception {
        Message mockMessage = mock(Message.class);
        ActiveMQQueue originalDest = new ActiveMQQueue("LENS.AUDIT.ALL");
        when(mockMessage.getDestination()).thenReturn(originalDest);
        when(mockMessage.getSize()).thenReturn(1024);

        auditBroker.send(mockExchange, mockMessage);

        verify(mockNext).send(mockExchange, mockMessage);
        verify(mockMessage, never()).copy(); // No audit message created
    }
}
