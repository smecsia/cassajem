package me.smecsia.cassajem;

import me.smecsia.cassajem.api.Conditions;
import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.service.Connection;

import java.util.List;

/**
 * Common interface for the entity manager
 * User: isadykov
 * Date: 30.01.12
 * Time: 18:34
 *
 * @param <T> Class of the managed entity
 */
public interface EntityManager<T extends BasicEntity> {
    /**
     * Returns current EM connection
     *
     * @return connection instance
     */
    public Connection getConnection();


    /**
     * Create Column Family if it's not exist
     */
    public void initColumnFamily();

    /**
     * Finds the row by its ID
     *
     * @param id row key value
     * @return managed entity instance for the given row id
     */
    public T find(Object id);

    /**
     * Finds the row by its ID and select only columnNames
     *
     * @param id          row key value
     * @param columnNames list of columns to read from column family
     * @return managed entity instance for the given row id
     */
    public T find(Object id, Object[] columnNames);

    /**
     * Finds the row by its ID and returns only part of the row using columns range
     *
     * @param from column names range start
     * @param to   colum names range end
     * @param id   row key value
     * @return managed entity instance for the given row id
     */
    public T find(Object from, Object to, Object id);

    /**
     * Saves entity instance into cassandra cluster
     *
     * @param object entity instance
     */
    public void save(T object);

    /**
     * Partial entity save, including only the presented fields
     *
     * @param includeFieldNames use only these fields for the columns
     * @param object            entity instance
     */
    public void save(T object, String... includeFieldNames);

    /**
     * Saves entity instance into cassandra cluster with save policy
     *
     * @param savePolicy defines the policy of mutation (REWRITE or APPEND)
     * @param object     entity instance
     */
    public void save(T object, ColumnFamily.SavePolicy savePolicy);

    /**
     * Finds the list of entities by their row keys
     *
     * @param ids row keys list
     * @return List of entity instances
     */
    public List<T> list(Object... ids);

    /**
     * Finds the list of entities by their row keys and using the columns range
     *
     * @param ids  row keys list
     * @param from start column range
     * @param to   end column range
     * @return List of entity instances
     */
    public List<T> list(Object[] ids, Object from, Object to);

    /**
     * Finds the list of entities by the condition
     *
     * @param conditions conditions
     * @return list of entity instances matching the condition
     */
    public List<T> filter(Conditions conditions);

    /**
     * Finds the list of entities by the condition
     *
     * @param from       column names range start
     * @param to         colum names range end
     * @param conditions conditions
     * @return list of entity instances matching the condition
     */
    public List<T> filter(Object from, Object to, Conditions conditions);

    /**
     * Finds the list of entities by the condition
     *
     * @param from       column names range start
     * @param to         colum names range end
     * @param conditions conditions
     * @param colLimit   maximum number of columns fetched per query
     * @return list of entity instances matching the condition
     */
    public List<T> filter(Object from, Object to, Conditions conditions, int colLimit);


    /**
     * Remove row by id
     *
     * @param id id of a row to be removed
     */
    public void remove(Object id);
}
