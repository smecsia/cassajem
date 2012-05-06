package me.smecsia.cassajem;

import me.smecsia.cassajem.api.BasicService;
import me.smecsia.cassajem.util.ClassUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.CollectionUtils;

import java.util.Properties;

/**
 * Class allowing to read all the configuration of the application
 */
public class Config extends BasicService {

    private static final String CONFIG_PATTERN = "classpath*:cassajem.properties";

    private static final String PROP_KEYSPACE = "me.smecsia.cassajem.keyspace";
    private static final String PROP_STRATEGY = "me.smecsia.cassajem.strategy";
    private static final String PROP_REPLFACT = "me.smecsia.cassajem.replicationFactor";
    private static final String PROP_CASCONFF = "me.smecsia.cassajem.cassandraConfig";
    private static final String PROP_EMFACTCS = "me.smecsia.cassajem.entityManagerFactory";
    private static final String PROP_ENTMGRCS = "me.smecsia.cassajem.entityManager";
    private static final String DEFAULT_CASS_CONF_FILE = "/cassandra.yaml";
    private static final String DEFAULT_CASS_STRATEGY = "SimpleStrategy";
    private static final String DEFAULT_ENTITYMGR_FACT = "me.smecsia.cassajem.EntityManagerFactoryImpl";
    private static final String DEFAULT_ENTITYMGR_CLSS = "me.smecsia.cassajem.EntityManagerImpl";
    private static final String DEFAULT_REPLIC_FACTOR = "1";

    private Class<EntityManager> cachedEmClass = null;
    private Class<EntityManagerFactory> cachedEmfClass = null;

    Properties configuration = new Properties();

    private static Config instance = null;

    private Config() {
        try {
            for (Resource res : ClassUtil.resolveResourcesFromPattern(CONFIG_PATTERN)) {
                Properties resProperties = new Properties();
                resProperties.load(res.getInputStream());
                CollectionUtils.mergePropertiesIntoMap(resProperties, configuration);
            }
        } catch (Exception e) {
            logAndThrow("Cannot initialize configuration: " + e.getMessage(), e);
        }
    }

    public synchronized static Config instance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public String get(String property) {
        return configuration.getProperty(property);
    }

    public String getKeyspaceName() {
        return get(PROP_KEYSPACE);
    }

    public String getStrategy() {
        return get(PROP_STRATEGY, DEFAULT_CASS_STRATEGY);
    }

    public Integer getReplicationFactor() {
        return Integer.valueOf(get(PROP_REPLFACT, DEFAULT_REPLIC_FACTOR));
    }

    public String get(String property, String defaultValue) {
        String value = get(property);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    public String getCassandraConfigFile() {
        return get(PROP_CASCONFF, DEFAULT_CASS_CONF_FILE);
    }

    /**
     * Returns the class which is defined in the configuration or the default value
     *
     * @return entity manager factory class
     */
    @SuppressWarnings("unchecked")
    public synchronized Class<EntityManagerFactory> getEntityManagerFactoryClass() {
        if (cachedEmfClass == null) {
            try {
                cachedEmfClass = (Class<EntityManagerFactory>) Class.forName(get(PROP_EMFACTCS, DEFAULT_ENTITYMGR_FACT));
            } catch (ClassNotFoundException e) {
                logAndThrow("Could not find the EntityManagerFactory class: " + e.getMessage(), e);
            }
        }
        return cachedEmfClass;
    }

    /**
     * Returns the class which is defined in the configuration or the default value
     *
     * @return entity manager class
     */
    @SuppressWarnings("unchecked")
    public synchronized Class<EntityManager> getEntityManagerClass() {
        if (cachedEmClass == null) {
            try {
                cachedEmClass = (Class<EntityManager>) Class.forName(get(PROP_ENTMGRCS, DEFAULT_ENTITYMGR_CLSS));
            } catch (ClassNotFoundException e) {
                logAndThrow("Could not find the EntityManager class: " + e.getMessage(), e);
            }
        }
        return cachedEmClass;
    }
}
