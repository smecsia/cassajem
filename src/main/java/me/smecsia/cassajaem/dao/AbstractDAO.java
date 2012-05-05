package me.smecsia.cassajaem.dao;

import me.smecsia.cassajaem.EntityManager;
import me.smecsia.cassajaem.api.BasicEntity;
import me.smecsia.cassajaem.api.CompositeKey;
import me.smecsia.cassajaem.api.CassajaemException;
import me.smecsia.cassajaem.meta.CompositeColumnArrayInfo;
import me.smecsia.cassajaem.meta.MetaCache;
import me.smecsia.cassajaem.meta.Metadata;
import me.smecsia.cassajaem.meta.annotations.EntityContext;
import me.smecsia.cassajaem.service.DAOFactory;
import me.smecsia.cassajaem.service.EntityManagerFactory;
import me.smecsia.cassajaem.util.Inflector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static me.smecsia.cassajaem.util.TypesUtil.compositeKey;
import static me.smecsia.cassajaem.util.TypesUtil.extractFromCompositeKeys;
import static me.smecsia.cassajaem.util.UUIDUtil.maxTimeUUID;
import static me.smecsia.cassajaem.util.UUIDUtil.minTimeUUID;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 03.02.12
 * Time: 17:05
 */
public abstract class AbstractDAO<T extends BasicEntity> implements DAO<T> {
    private EntityManager<T> entityManager = null;

    protected EntityContext entityContext;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected EntityManagerFactory entityManagerFactory;

    protected DAOFactory daoFactory;

    protected static final List plainEmptyList = Collections.emptyList();

    protected Class<? extends BasicEntity> entityClass() {
        return this.entityContext.entityClass();
    }

    protected AbstractDAO(EntityManagerFactory entityManagerFactory, DAOFactory daoFactory) {
        this.entityContext = getClass().getAnnotation(EntityContext.class);
        this.entityManagerFactory = entityManagerFactory;
        this.daoFactory = daoFactory;
    }

    /**
     * Create the new DAO (proxy method to daoFactory)
     *
     * @param entityClass class of an entity
     * @param <C> type of an entity
     * @return newly created DAO
     */
    protected <C extends BasicEntity> DAO<C> getDAO(Class<C> entityClass) {
        return daoFactory.getDAO(entityClass);
    }

    /**
     * Returns entity field name by convention
     * 
     * @return plural entity name (Ex: Event -> events, Visit -> visits)
     */
    protected String entityFieldNamePlural() {
        return Inflector.getInstance().pluralize(entityClass().getSimpleName().toLowerCase());
    }

    /**
     * Returns the maximum possible composite key, when the entity is used as field
     * 
     * @return composite key (Ex: &lt;events:FFF...FFF&gt;)
     */
    protected CompositeKey maxKey() {
        return compositeKey(entityFieldNamePlural(), maxTimeUUID());
    }

    /**
     * Returns the minimum possible composite key, when entity is used as field
     *
     * @return composite key (Ex: &lt;events:0000-00...000&gt;)
     */
    protected CompositeKey minKey() {
        return compositeKey(entityFieldNamePlural(), minTimeUUID());
    }

    @SuppressWarnings("unchecked")
    protected List loadListData(Collection ids) {
        return byId(ids.toArray());
    }

    @Override
    public T byId(Object from, Object to, Object id) {
        return entityManager().find(from, to, id);
    }

    @Override
    public List<T> byId(Object from, Object to, Object[] ids) {
        return entityManager().list(ids, from, to);
    }

    @Override
    public List<T> byId(Object[] ids) {
        return entityManager.list(ids);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T byIdOrNew(Object id) {
        return byIdOrNew(null, null, id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T newInstance(Object id) {
        T res;
        try {
            Metadata<? extends BasicEntity> md = MetaCache.instance().forClass(entityClass());
            res = (T) entityClass().newInstance();
            md.getIdColumn().invokeSetter(res, id);
        } catch (Exception e) {
            throw new CassajaemException(e.getMessage());
        }
        return res;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T byIdOrNew(Object from, Object to, Object id) {
        T res = entityManager().find(from, to, id);
        if (res == null) {
            res = newInstance(id);
        }
        return res;
    }

    /**
     * Return entities by parent class
     * 
     * @param parentClazz class of parent (Ex: Identity.class)
     * @param parentId parent class id (Ex: identityId)
     * @return list of pages
     */
    @SuppressWarnings("unchecked")
    public List byParent(Class<? extends BasicEntity> parentClazz, Object parentId) {
        Metadata md = MetaCache.instance().forClass(parentClazz);
        BasicEntity obj = getDAO(parentClazz).byIdOrNew(minKey(), maxKey(), parentId);
        CompositeColumnArrayInfo cInfo = (CompositeColumnArrayInfo) md.getCompositeArrays()
                .get(entityFieldNamePlural());
        return loadListData((List<UUID>) extractFromCompositeKeys(((Map) cInfo.invokeGetter(obj)).keySet(), 1));
    }

    @Override
    public T byId(Object id, Object[] columnNames) {
        return entityManager().find(id);
    }

    @Override
    public T byId(Object id) {
        return entityManager().find(id);
    }

    @Override
    public void create(T object) {
        entityManager().save(object);
    }

    @Override
    public void save(T object, String... includeOnlyFields) {
        entityManager().save(object, includeOnlyFields);
    }

    @Override
    public void save(T object) {
        entityManager().save(object);
    }

    @Override
    public void remove(Object id) {
        entityManager().remove(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public EntityManager<T> entityManager() {
        if (entityManager == null) {
            this.entityManager = entityManagerFactory.createEntityManager((Class<T>) entityClass());
        }
        return this.entityManager;
    }
}
