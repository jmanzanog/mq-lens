package com.jmanzano.mqlens.plugin;

import org.apache.activemq.broker.Broker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class LensAuditPluginTest {

    @Test
    public void testInstallPluginEnabled() throws Exception {
        LensAuditPlugin plugin = new LensAuditPlugin();
        Broker mockBroker = mock(Broker.class);
        Broker installed = plugin.installPlugin(mockBroker);
        
        assertTrue(installed instanceof LensAuditBroker);
    }

    @Test
    public void testInstallPluginDisabled() throws Exception {
        LensAuditPlugin plugin = new LensAuditPlugin();
        plugin.setEnabled(false);
        Broker mockBroker = mock(Broker.class);
        Broker installed = plugin.installPlugin(mockBroker);
        
        assertEquals(mockBroker, installed); // Bypassed
    }
}
