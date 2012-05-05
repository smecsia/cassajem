package me.smecsia.cassajaem.service;

import me.smecsia.cassajaem.api.BasicService;
import me.smecsia.cassajaem.api.CassajaemInitializationException;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.URL;

/**
 * Embedded Cassandra instance runner
 *
 * @author ilyasadykov
 */
public class EmbeddedCassandraService extends BasicService {

    private Boolean initialized = false;

    private CassandraDaemon cassandraDaemon;

    public EmbeddedCassandraService(String cassandraConfigFile) {
        if (cassandraConfigFile == null) {
            throw new CassajaemInitializationException("Embedded Cassandra configuration file path cannot be null! ");
        }
        URL configRes = getClass().getResource(cassandraConfigFile);
        if (configRes == null) {
            throw new CassajaemInitializationException("Cannot start embedded Cassandra instance with the config file: " + cassandraConfigFile);
        }
        System.setProperty("cassandra.config", configRes.toString());
        System.setProperty("log4j.defaultInitOverride", "false");
        System.setProperty("cassandra-foreground", "true");
    }

    /**
     * Initialize cassandra service
     *
     * @throws TTransportException inner cassandra daemon exception
     * @throws IOException         inner cassandra daemon exception
     */
    public synchronized void init() throws TTransportException, IOException {
        logger.info("Activating Embedded Cassandra daemon...");
        if (!initialized) {
            try {
                cassandraDaemon = new CassandraDaemon();
                cassandraDaemon.activate();
                initialized = true;
            } catch (Exception e) {
                logger.error("Cannot start embedded Cassandra instance! " + e.getMessage());
            }
        } else
            logAndThrow("Cassandra embedded instance already initialized!");
    }

    /**
     * Deactivates embedded cassandra service
     */
    public synchronized void deactivate() {
        try {
            cassandraDaemon.deactivate();
        } catch (Exception e) {
            logAndThrow("Cannot stop Cassandra instance: " + e.getMessage());
            throw new RuntimeException(e);
        }

    }

    public boolean isRunning() {
        return isInitialized() && cassandraDaemon.isRPCServerRunning();
    }

    public Boolean isInitialized() {
        return initialized;
    }
}
