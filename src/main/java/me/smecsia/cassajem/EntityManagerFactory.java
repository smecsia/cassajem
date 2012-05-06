package me.smecsia.cassajem;

import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.service.Connection;

/**
 * Entity manager factory
 */
public interface EntityManagerFactory {

    public <T extends BasicEntity> EntityManager<T> createEntityManager(Class<T> entityClass);

    public <T extends BasicEntity> EntityManager<T> getEntityManager(Class<T> entityClass);

    public void setConnection(Connection connection);

}
