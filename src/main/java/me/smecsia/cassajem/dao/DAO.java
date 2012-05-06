package me.smecsia.cassajem.dao;

import me.smecsia.cassajem.EntityManager;
import me.smecsia.cassajem.api.BasicEntity;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: smecsia
 * Date: 03.02.12
 * Time: 17:05
 */
public interface DAO<T extends BasicEntity> {
    /**
     * Find an entity by its row key
     *
     * @param id - id of the entity
     * @return entity instance
     */
    public T byId(Object id);

    /**
     * Find the entities list by the row keys and returns using columns filter
     *
     * @param id          id of the entity
     * @param columnNames list of the columns to read from column family
     * @return entity instance
     */
    public T byId(Object id, Object[] columnNames);

    /**
     * Find the entities list by the row keys and returns using columns filter
     *
     * @param from from column name
     * @param to   to column name
     * @param id   id of the entity
     * @return entity instance
     */
    public T byId(Object from, Object to, Object id);

    /**
     * Find the entities list by the row keys and returns using columns filter
     *
     * @param from from column name
     * @param to   to column name
     * @param id   ids of the entities
     * @return entity instance
     */
    public List<T> byId(Object from, Object to, Object[] id);

    /**
     * Find the entities list by the row keys
     *
     * @param id - id of the entity
     * @return entity instance
     */
    public List<T> byId(Object[] id);

    /**
     * Find an entity by id or create new instance
     *
     * @param id - id of the entity
     * @return new instance if nothing is found, or found entity instance otherwise
     */
    public T byIdOrNew(Object id);


    /**
     * Find an entity by id or create new instance
     *
     * @param from lower columns range bound
     * @param to   upper columns range bound
     * @param id   id of the entity
     * @return new instance if nothing is found, or found entity instance otherwise
     */
    public T byIdOrNew(Object from, Object to, Object id);


    /**
     * Create new instance of an object
     *
     * @param id object identifier
     * @return new instance
     */
    public T newInstance(Object id);

    /**
     * Save entity instance as new row
     *
     * @param object - entity instance
     */
    public void create(T object);

    /**
     * Save entity instance, but include only the certain fields (Partial save)
     *
     * @param object            entity instance
     * @param includeOnlyFields fields to be included
     */
    public void save(T object, String... includeOnlyFields);

    /**
     * Save entity instance
     *
     * @param object entity instance
     */
    public void save(T object);

    /**
     * Remove row from a CF by id
     *
     * @param id id of a row
     */
    public void remove(Object id);

    /**
     * Returns current entity manager of this DAO
     *
     * @return entity manager
     */
    public EntityManager<T> entityManager();
}
