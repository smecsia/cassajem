package me.smecsia.cassajaem.service;

import me.smecsia.cassajaem.api.BasicEntity;
import me.smecsia.cassajaem.api.BasicService;
import me.smecsia.cassajaem.dao.DAO;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for the data access objects
 * User: isadykov
 * Date: 02.02.12
 * Time: 17:34
 */
public class DAOFactory extends BasicService {
    private String basePackage;

    private final Map<Class<?>, DAO<? extends BasicEntity>> daoCache = new ConcurrentHashMap<Class<?>,
            DAO<? extends BasicEntity>>();

    public DAOFactory(String basePackage) {
        this.basePackage = basePackage;
    }

    /**
     * Create new DAO
     * TODO: Refactor the logic please
     * 
     * @param entityClass class name
     * @param <T> type of entity
     * @return new DAO
     */
    @SuppressWarnings("unchecked")
    public <T extends BasicEntity> DAO<T> getDAO(Class<T> entityClass) {
        if (!daoCache.containsKey(entityClass)) {
            logger.info("DAOFactory: DAO for entityClass '" + entityClass.getName()
                    + "' not found in cache, creating the new instance...");
            try {
                Class<? extends DAO> daoClass = (Class<? extends DAO>) Class.forName(basePackage + "."
                        + entityClass.getSimpleName() + "DAO");
                daoCache.put(entityClass, daoClass.newInstance());
            } catch (Exception e) {
                logAndThrow("DAOFactory: Class " + entityClass.getSimpleName() + " not found in package " + basePackage
                        + " ! Cannot create DAO!");
                e.printStackTrace();
            }
        }
        return (DAO<T>) daoCache.get(entityClass);
    }
}
