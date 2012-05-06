package me.smecsia.cassajem.service;

import me.smecsia.cassajem.Config;
import me.smecsia.cassajem.EntityManager;
import me.smecsia.cassajem.EntityManagerFactory;
import me.smecsia.cassajem.api.BasicService;
import me.smecsia.cassajem.api.CassajemInitializationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;

/**
 * Embedded Cassandra instance runner
 *
 * @author ilyasadykov
 */
public class EmbeddedCassandraService extends BasicService {

    private Boolean initialized = false;

    private CassandraDaemon cassandraDaemon;

    private Constructor emfContructor = null;

    private Connection cachedConnection = null;

    public EmbeddedCassandraService(String cassandraConfigFile) {
        if (cassandraConfigFile == null) {
            throw new CassajemInitializationException("Embedded Cassandra configuration file path cannot be null! ");
        }
        URL configRes = getClass().getResource(cassandraConfigFile);
        if (configRes == null) {
            throw new CassajemInitializationException("Cannot start embedded Cassandra instance with the config file: " + cassandraConfigFile);
        }
        System.setProperty("cassandra.config", configRes.toString());
        System.setProperty("log4j.defaultInitOverride", "false");
        System.setProperty("cassandra-foreground", "true");
    }

    public EmbeddedCassandraService() {
        this(Config.instance().getCassandraConfigFile());
    }

    public Connection getConnection() {
        if (cachedConnection == null) {
            cachedConnection = new Connection(
                    DatabaseDescriptor.getClusterName(),
                    DatabaseDescriptor.getListenAddress().getHostName() + ":" + DatabaseDescriptor.getRpcPort(),
                    Config.instance().getKeyspaceName(),
                    Config.instance().getStrategy(),
                    Config.instance().getReplicationFactor());
            cachedConnection.connect();
        }
        return cachedConnection;
    }

    @SuppressWarnings("unchecked")
    public EntityManagerFactory createEntityManagerFactory() {
        EntityManagerFactory factory = null;
        try {
            if (emfContructor == null) {
                emfContructor = Config.instance().getEntityManagerFactoryClass().getConstructor(Connection.class, EntityManager.class.getClass());
            }
            factory = (EntityManagerFactory) emfContructor.newInstance(getConnection(), Config.instance().getEntityManagerClass());
        } catch (Exception e) {
            logAndThrow("Error creating entity manager factory: " + e.getMessage(), e);
        }
        return factory;
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
                logAndThrow("Cannot start embedded Cassandra instance! " + e.getMessage(), e);
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
            logAndThrow("Cannot stop Cassandra instance: " + e.getMessage(), e);
        }

    }

    public boolean isRunning() {
        return isInitialized() && cassandraDaemon.isRPCServerRunning();
    }

    public Boolean isInitialized() {
        return initialized;
    }
}
