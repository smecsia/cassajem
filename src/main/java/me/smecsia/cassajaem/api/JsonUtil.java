package me.smecsia.cassajaem.api;

/**
 * Basic interface allowing to serialize/deserialize json
 *
 *
 * Date: 4/9/12
 * Time: 6:06 PM
 *
 */
public interface JsonUtil {

    /**
     * Restore from json
     *
     * @param jsonString  json string
     * @param entityClass entity type
     * @param <C>         entity type
     * @return deserialized object
     */
    public <C extends BasicEntity> C fromJson(String jsonString, Class<C> entityClass);

    /**
     * Serialize object to json
     *
     * @param obj object instance
     * @return serialized object
     */
    public String toJson(Object obj);

    /**
     * Returns JSON field value for the chain of json structure.
     * Example:
     * String json = "{\"a\": {\"b\" : 10}}";
     * getStringSubValue(json, "a", "b"); // gives "10"
     *
     * @param json        json string
     * @param fieldsChain fields chain
     * @return string value of the desired field
     */
    String getStringSubValue(String json, String... fieldsChain);

    /**
     * Get json node from jackson
     *
     * @param json json
     * @return node
     */
    Iterable fromJson(String json);


    /**
     * Create empty json node
     *
     * @return node
     */
    Iterable createObjectNode();
}
