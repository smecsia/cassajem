package me.smecsia.cassajem.api;

import me.smecsia.cassajem.util.TypesUtil;
import me.prettyprint.hector.api.Serializer;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Class which is used to define the search conditions (Cassandra filter expression wrapper)
 * User: smecsia
 * Date: 21.02.12
 * Time: 17:56
 */
public class Conditions {

    /**
     * Basic condition item
     */
    public static class ConditionItem {
        public Object property;
        public IndexOperator operation = IndexOperator.EQ;
        public Object value;

        public ConditionItem(Object prop, IndexOperator operation, Object value) {
            this.property = prop;
            this.operation = operation;
            this.value = value;
        }

        @Override
        public String toString() {
            return "[" + property + " " + operation + " '" + value + "']";
        }
    }

    private List<ConditionItem> operations = new ArrayList<ConditionItem>();
    private Serializer valueSerializer = TypesUtil.stringSerializer;

    /**
     * Add a new condition into the list of conditions
     *
     * @param prop  property name (column name)
     * @param cond  operator (EQ,GTE,GT,LT,LTE)
     * @param value value of comparision
     * @return this conditions object
     */
    @SuppressWarnings("unchecked")
    public Conditions add(Object prop, IndexOperator cond, Object value) {
        if (!(value instanceof String)) {
            valueSerializer = TypesUtil.byteBufferSerializer;

        }
        this.operations.add(new ConditionItem(prop, cond, value));
        return this;
    }

    public Conditions() {

    }

    public Conditions(Object property, IndexOperator cond, Object value) {
    }

    public Conditions(Object property, Object value) {
        eq(property, value);
    }

    public List<ConditionItem> operations() {
        return operations;
    }

    public static Conditions cond() {
        return new Conditions();
    }

    public Serializer getValueSerializer() {
        return valueSerializer;
    }

    /**
     * Add equals condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions eq(Object prop, Object value) {
        return add(prop, IndexOperator.EQ, value);
    }

    /**
     * Add lesser or equals condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions lteq(Object prop, Object value) {
        return add(prop, IndexOperator.LTE, value);
    }

    /**
     * Add lesser condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions lt(Object prop, Object value) {
        return add(prop, IndexOperator.LT, value);
    }

    /**
     * Add greater condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions gt(Object prop, Object value) {
        return add(prop, IndexOperator.GT, value);
    }

    /**
     * Add greater or equals condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions gteq(Object prop, Object value) {
        return add(prop, IndexOperator.GTE, value);
    }

    @Override
    public String toString() {
        return "'" + StringUtils.join(operations, "', '") + "'";
    }
}
