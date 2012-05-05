package me.smecsia.cassajaem.service;

import com.genesyslab.wme.common.application.BasicService;
import me.prettyprint.cassandra.model.BasicKeyspaceDefinition;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

/**
 * Cassandra connection. Wrapper for the cluster & keyspace of Hector API
 * Identity: isadykov
 * Date: 18.01.12
 * Time: 16:27
 */
@Lazy
@Service("database.connection")
public class Connection extends BasicService {
    // current configuration
    protected String clusterName;
    protected String clusterHost;
    protected String keyspaceName;
    protected String strategy;
    protected Integer replicationFactor;

    // Pelops fields
    protected Cluster cluster = null;
    protected Keyspace keyspace = null;

    @Autowired
    public Connection(@Value("#{databaseConfig.conf('wmdb.client.clusterName')}") String clusterName,
                      @Value("#{databaseConfig.conf('wmdb.client.clusterHost')}") String clusterHost,
                      @Value("#{databaseConfig.conf('wmdb.client.keyspaceName')}") String keyspaceName,
                      @Value("#{databaseConfig.conf('wmdb.client.defaultStrategy')}") String strategy,
                      @Value("#{databaseConfig.conf('wmdb.client.defaultReplicationFactor')}")
                      Integer replicationFactor) {
        this.clusterName = clusterName;
        this.clusterHost = clusterHost;
        this.keyspaceName = keyspaceName;
        this.strategy = strategy;
        this.replicationFactor = replicationFactor;
    }

    /**
     * Returns true if connected
     *
     * @return true if connection is active
     */
    public boolean isConnected() {
        return cluster != null;
    }

    /**
     * Connects to a Cassandra cluster with a given configuration
     */
    public synchronized void connect() {
        if (!isConnected()) {
            cluster = HFactory.getOrCreateCluster(clusterName, clusterHost);
            createKeySpaceIfNotExist();
            keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        } else logAndThrow("WMDB Connection already initiated!");
    }

    /**
     * Disconnects from a Cassandra cluster
     */
    public synchronized void disconnect() {
        if (cluster != null) {
            cluster.getConnectionManager().shutdown();
            keyspace = null;
            cluster = null;
        }
    }

    /**
     * Drops the keyspace if it exists
     */
    public void dropKeySpaceIfExists() {
        if (cluster != null) {
            KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
            if (keyspaceDef != null) {
                cluster.dropKeyspace(keyspaceName);
            }
        }
    }

    /**
     * Creates the keyspace if it does not exist
     */
    public void createKeySpaceIfNotExist() {
        if (cluster != null) {
            KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
            if (keyspaceDef == null) {
                BasicKeyspaceDefinition ksDef = new BasicKeyspaceDefinition();
                ksDef.setName(keyspaceName);
                ksDef.setReplicationFactor(replicationFactor);
                ksDef.setStrategyClass(strategy);
                cluster.addKeyspace(ksDef);
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }
}
