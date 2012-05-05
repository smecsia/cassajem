package me.smecsia.cassajem.api;

import static me.smecsia.cassajem.util.TypesUtil.serializerByType;

/**
 * Hector composite wrapper. Composite column name that uses the specified list of the types and the specified values
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:43
 */
public class CompositeKey extends me.prettyprint.hector.api.beans.Composite {
    protected Class<?>[] types;
    protected Object[] values;

    public CompositeKey() {
    }

    public CompositeKey(Class<?>... types) {
        this.types = types;
    }

    public Class<?>[] getTypes() {
        return types;
    }

    /**
     * Sets the inner values for this key
     *
     * @param values - values array
     * @return self
     */
    @SuppressWarnings("unchecked")
    public final CompositeKey setValues(Object... values) {
        for (int i = 0; i < values.length; ++i) {
            if (!types[i].isAssignableFrom(values[i].getClass())) {
                throw new CassajaemException("Wrong argument type '" + values[i].getClass() + "' for the type '" + types[i] + "'!");
            }
            this.addComponent(values[i], serializerByType(types[i]));
        }
        this.values = values.clone();
        return this;
    }

    /**
     * Sets the inner types for this key
     *
     * @param types - array
     */
    public final void setTypes(Class<?>[] types) {
        this.types = types;
    }

    /**
     * Set the values (can be overriden)
     *
     * @param values - values array
     * @return self
     */
    @SuppressWarnings("unchecked")
    public CompositeKey set(Object... values) {
        return setValues(values);
    }
}
