package me.smecsia.cassajem.dao;

import me.smecsia.cassajem.EntityManagerFactory;
import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.api.BasicService;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for the data access objects
 * User: isadykov
 * Date: 02.02.12
 * Time: 17:34
 */
public class DAOFactory extends BasicService {
    private String basePackage = "me.smecsia.cassajem.dao";

    private EntityManagerFactory entityManagerFactory;

    private final Map<Class<?>, DAO<? extends BasicEntity>> daoCache = new ConcurrentHashMap<Class<?>,
            DAO<? extends BasicEntity>>();

    public DAOFactory(EntityManagerFactory emf, String basePackage) {
        this.entityManagerFactory = emf;
        this.basePackage = basePackage;
    }

    public DAOFactory(EntityManagerFactory emf) {
        this.entityManagerFactory = emf;
    }


    /**
     * Create new DAO
     * TODO: Refactor the logic please
     *
     * @param entityClass class name
     * @param <T>         type of entity
     * @return new DAO
     */
    @SuppressWarnings("unchecked")
    public <T extends BasicEntity> DAO<T> getDAO(Class<T> entityClass) {
        if (!daoCache.containsKey(entityClass)) {
            logger.info("DAOFactory: DAO for entityClass '" + entityClass.getName()
                    + "' not found in cache, creating the new instance...");
            try {
                try {
                    String sep = (basePackage.endsWith("$")) ? "" : ".";
                    Class<? extends DAO> daoClass = (Class<? extends DAO>) Class.forName(basePackage + sep
                            + entityClass.getSimpleName() + "DAO");
                    try {
                        Constructor<DAO<? extends BasicEntity>> constructor = (Constructor<DAO<? extends BasicEntity>>)
                                daoClass.getConstructor(EntityManagerFactory.class, DAOFactory.class, BasicEntity.class.getClass());
                        daoCache.put(entityClass, constructor.newInstance(entityManagerFactory, this, entityClass));
                    } catch (NoSuchMethodException threeacE) {
                        try {
                            Constructor<DAO<? extends BasicEntity>> constructor = (Constructor<DAO<? extends BasicEntity>>)
                                    daoClass.getConstructor(EntityManagerFactory.class, DAOFactory.class);
                            daoCache.put(entityClass, constructor.newInstance(entityManagerFactory, this));
                        } catch (NoSuchMethodException twoacE) {
                            try {
                                daoCache.put(entityClass, daoClass.newInstance());
                            } catch (Exception noacE) {
                                logAndThrow("Cannot find no-args/2-args nor 3-args constructor within the DAO class: " + daoClass.getName());
                            }
                        }
                    }
                } catch (ClassNotFoundException cnfE) {
                    daoCache.put(entityClass, new AbstractDAOImpl(entityManagerFactory, this, entityClass));
                }
            } catch (Exception e) {
                logAndThrow("DAOFactory: Cannot create DAO! Check that class " + entityClass.getSimpleName() +
                        " can be found in package " + basePackage + " and it have the default constructor with 3 args!");
                e.printStackTrace();
            }
        }
        return (DAO<T>) daoCache.get(entityClass);
    }
}
