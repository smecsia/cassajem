package me.smecsia.cassajem;

import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.api.BasicService;
import me.smecsia.cassajem.api.CassajemException;
import me.smecsia.cassajem.service.Connection;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates the entity manager for the given entity class
 * User: isadykov
 * Date: 02.02.12
 * Time: 17:34
 */
public class EntityManagerFactoryImpl extends BasicService implements EntityManagerFactory {
    private Connection connection = null;

    private Constructor emInstanceConstructor;

    private Map<Class<? extends BasicEntity>, EntityManager<? extends BasicEntity>> emCache = new
            ConcurrentHashMap<Class<? extends BasicEntity>, EntityManager<? extends BasicEntity>>();

    protected synchronized void checkInitConnection() {
        if (connection == null || !connection.isConnected()) {
            logAndThrow(new CassajemException("Not connected to a Cassandra cluster!"));
        }
    }

    public EntityManagerFactoryImpl(Connection connection, Class<EntityManager> entityManagerClass) {
        this.connection = connection;
        try {
            emInstanceConstructor = entityManagerClass.getConstructor(Connection.class, BasicEntity.class.getClass());
        } catch (NoSuchMethodException e) {
            logAndThrow("Cannot initialize EntityManagerFactoryImpl with the provided EM implementation class: '"
                    + entityManagerClass.getName() + "': " + e.getMessage(), e);
        }
    }

    /**
     * Create new entity manager for the provided class
     *
     * @param entityClass clazz
     * @param <T>         type of an entity
     * @return newly created entity manager
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends BasicEntity> EntityManager<T> createEntityManager(Class<T> entityClass) {
        checkInitConnection();
        EntityManager<T> res = null;
        try {
            res = (EntityManager<T>) emInstanceConstructor.newInstance(connection, entityClass);
            res.initColumnFamily();
        } catch (Exception e) {
            e.printStackTrace();
            logAndThrow("Cannot initialize EntityManager for provided class: '" + entityClass.getName() + "': "
                    + e.getMessage(), e);
        }
        return res;
    }


    /**
     * Return entity manager from cache or create the new one
     *
     * @param entityClass clazz
     * @param <T>         type of an entity
     * @return instance of entity manager for the class
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends BasicEntity> EntityManager<T> getEntityManager(Class<T> entityClass) {
        if (!emCache.containsKey(entityClass)) {
            emCache.put(entityClass, createEntityManager(entityClass));
        }
        return (EntityManager<T>) emCache.get(entityClass);
    }

    /**
     * Set the connection for this entity manager factory
     *
     * @param connection connection
     */
    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
