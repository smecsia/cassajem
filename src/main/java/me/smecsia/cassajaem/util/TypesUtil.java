package me.smecsia.cassajaem.util;

import me.smecsia.cassajaem.util.DateUtil;
import me.smecsia.cassajaem.util.StringUtil;
import me.smecsia.cassajaem.util.UUIDUtil;
import me.smecsia.cassajaem.api.CompositeKey;
import me.smecsia.cassajaem.api.EventsCompositeKey;
import me.smecsia.cassajaem.api.UUIDCompositeKey;
import me.smecsia.cassajaem.entity.Event;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:51
 */
public class TypesUtil {
    public static final UUIDSerializer uuidSerializer = UUIDSerializer.get();
    public static final StringSerializer stringSerializer = StringSerializer.get();
    public static final ByteBufferSerializer byteBufferSerializer = ByteBufferSerializer.get();
    public static final LongSerializer longSerializer = LongSerializer.get();
    public static final IntegerSerializer integerSerializer = IntegerSerializer.get();
    public static final DateSerializer dateSerializer = DateSerializer.get();
    public static final FloatSerializer floatSerializer = FloatSerializer.get();
    public static final DoubleSerializer doubleSerializer = DoubleSerializer.get();
    public static final CompositeSerializer compositeSerializer = new CompositeSerializer();
    public static final BooleanSerializer booleanSerializer = BooleanSerializer.get();
    public static final ComparatorType bytesComparator = ComparatorType.BYTESTYPE;
    public static final ComparatorType compositeComparator = ComparatorType.COMPOSITETYPE;
    public static final ComparatorType defaultComparator = bytesComparator;
    public static final Class<?> defaultValidationClass = BytesType.class;


    /**
     * Returns type of the comparator by the java type
     *
     * @param type - java type
     * @return cassandra comparator type
     */
    public static ComparatorType comparatorTypeByType(Class<?> type) {
        if (UUID.class.isAssignableFrom(type)) {
            return ComparatorType.UUIDTYPE;
        }
        if (isInt(type)) {
            return ComparatorType.INTEGERTYPE;
        }
        if (isLong(type)) {
            return ComparatorType.LONGTYPE;
        }
        if (String.class.isAssignableFrom(type)) {
            return ComparatorType.UTF8TYPE;
        }
        return defaultComparator;
    }

    /**
     * Checks if the type is integer
     *
     * @param type - java type
     * @return true if the given type is integer
     */
    public static boolean isInt(Class<?> type) {
        return Integer.class.isAssignableFrom(type) || (type.isPrimitive() && type.toString().equals("int"));

    }

    /**
     * Checks if the type is double
     *
     * @param type - java type
     * @return true if the given type is double
     */
    public static boolean isDouble(Class<?> type) {
        return Double.class.isAssignableFrom(type) || (type.isPrimitive() && type.toString().equals("double"));
    }

    /**
     * Checks if the type is float
     *
     * @param type - java type
     * @return true if the given type is float
     */
    public static boolean isFloat(Class<?> type) {
        return Float.class.isAssignableFrom(type) || (type.isPrimitive() && type.toString().equals("float"));
    }

    /**
     * Checks if the type is Boolean
     *
     * @param type - java type
     * @return true if the given type is boolean
     */
    public static boolean isBoolean(Class<?> type) {
        return Boolean.class.isAssignableFrom(type) || (type.isPrimitive() && type.toString().equals("boolean"));
    }

    /**
     * Checks if the type is long
     *
     * @param type - java type
     * @return true if the given type is long
     */
    public static boolean isLong(Class<?> type) {
        return Long.class.isAssignableFrom(type) || (type.isPrimitive() && type.toString().equals("long"));
    }

    /**
     * Returns class validation by java type
     *
     * @param type               - java type
     * @param useTimeUUIDForUUID - set to true if you want to get TimeUUIDType for each UUID java type
     * @return validation class type
     */
    public static Class validationClassByType(Class<?> type, boolean useTimeUUIDForUUID) {
        if (String.class.isAssignableFrom(type)) {
            return UTF8Type.class;
        }
        if (isLong(type)) {
            return LongType.class;
        }
        if (isInt(type)) {
            return IntegerType.class;
        }
        if (UUID.class.isAssignableFrom(type)) {
            return (useTimeUUIDForUUID) ? TimeUUIDType.class : UUIDType.class;
        }
        if (isBoolean(type)) {
            return BooleanType.class;
        }
        return defaultValidationClass;
    }

    /**
     * returns class validation by java type
     *
     * @param type - java type
     * @return - CAssandra validation type
     */
    public static Class validationClassByType(Class<?> type) {
        return validationClassByType(type, true);
    }

    /**
     * Returns serializer by java type
     *
     * @param type - java type
     * @return - cassandra serializer
     */
    public static Serializer serializerByType(Class<?> type) {
        if (Composite.class.isAssignableFrom(type)) {
            return compositeSerializer;
        }
        if (String.class.isAssignableFrom(type)) {
            return stringSerializer;
        }
        if (UUID.class.isAssignableFrom(type)) {
            return uuidSerializer;
        }
        if (isLong(type)) {
            return longSerializer;
        }
        if (isInt(type)) {
            return integerSerializer;
        }
        if (Date.class.isAssignableFrom(type)) {
            return dateSerializer;
        }
        if (isFloat(type)) {
            return floatSerializer;
        }
        if (isDouble(type)) {
            return doubleSerializer;
        }
        if (isBoolean(type)) {
            return booleanSerializer;
        }
        return stringSerializer;
    }

    /**
     * Returns Cassandra comparator alias string by type
     *
     * @param type               - class type
     * @param useTimeUUIDForUUID - if you want to use TimeUUIDType for UUID class check this to true
     * @return - alias
     */
    public static String toComparatorAlias(Class type, boolean useTimeUUIDForUUID) {
        if (String.class.isAssignableFrom(type)) {
            return "UTF8Type";
        }
        if (UUID.class.isAssignableFrom(type)) {
            return (useTimeUUIDForUUID) ? "TimeUUIDType" : "UUIDType";
        }
        if (isLong(type)) {
            return "LongType";
        }
        if (isInt(type)) {
            return "IntegerType";
        }
        if (isBoolean(type)) {
            return "BooleanType";
        }
        return "BytesType";
    }

    /**
     * Returns Cassandra comparator alias string by composite types list
     *
     * @param useTimeUUIDForUUID - set this to true if you want to use TimeUUIDType for UUID java type
     * @param types              - types list
     * @return - alias
     */
    public static String toComparatorAlias(boolean useTimeUUIDForUUID, Class... types) {
        List<String> res = new ArrayList<String>();
        for (Class type : types) {
            res.add(toComparatorAlias(type, useTimeUUIDForUUID));
        }
        return "(" + StringUtils.join(res, ", ") + ")";
    }

    /**
     * Returns Cassandra comparator alias string by composite types list
     *
     * @param types - types list
     * @return - alias
     */
    public static String toComparatorAlias(Class... types) {
        return toComparatorAlias(true, types);
    }

    /**
     * REturns the void value by java type
     *
     * @param type - java type
     * @return void value
     */
    public static Object voidValue(Class<?> type) {
        if (String.class.isAssignableFrom(type)) {
            return DateUtil.timestampStr();
        }
        if (UUID.class.isAssignableFrom(type)) {
            return UUIDUtil.timeUUID();
        }
        if (isLong(type)) {
            return DateUtil.timestamp();
        }
        if (isInt(type)) {
            return 0;
        }
        if (isBoolean(type)){
            return false;
        }
        return null;
    }

    /**
     * Returns new uuid composite key by event
     *
     * @param prefix - prefix of uuid composite
     * @param value  - uuid value
     * @return new instance of uuid composite key
     */
    public static UUIDCompositeKey uuidKey(String prefix, UUID value) {
        return new UUIDCompositeKey(prefix, value);
    }

    /**
     * Returns new event composite key by event
     *
     * @param event - event instance
     * @return new instance of eventsCompositeKey
     */
    public static EventsCompositeKey eventKey(Event event) {
        return new EventsCompositeKey(event.getEventType(),
                event.getEventName(), event.getEventID());

    }

    /**
     * Returns new event composite key maximal
     *
     * @param eventId event timeUUID
     * @return new instance of eventsCompositeKey
     */
    public static EventsCompositeKey maxEventKey(UUID eventId) {
        return new EventsCompositeKey(StringUtil.maxString(), StringUtil.maxString(), eventId);
    }

    /**
     * Returns new event composite key minimal
     *
     * @param eventId event timeUUID
     * @return new instance of eventsCompositeKey
     */
    public static EventsCompositeKey minEventKey(UUID eventId) {
        return new EventsCompositeKey(StringUtil.minString(), StringUtil.minString(), eventId);
    }

    /**
     * Returns new event composite key by event name and type
     *
     * @param eventName event name
     * @param eventType event type
     * @param eventId   event timeUUID
     * @return new instance of eventsCompositeKey
     */
    public static EventsCompositeKey eventKey(String eventType, String eventName, UUID eventId) {
        return new EventsCompositeKey(eventType, eventName, eventId);
    }

    /**
     * Returns new composite key
     *
     * @param types  - list of java types
     * @param values - list of values
     * @return new instance of composite key
     */
    public static CompositeKey compositeKey(Class[] types, Object... values) {
        return compositeKey(CompositeKey.class, types, values);
    }

    /**
     * Returns new composite key
     *
     * @param clazz  - java type of composite key
     * @param types  - list of java types
     * @param values - list of values
     * @return new instance of composite key
     */
    public static CompositeKey compositeKey(Class<? extends CompositeKey> clazz, Class[] types, Object... values) {
        CompositeKey res = null;
        try {
            res = clazz.newInstance();
            res.setTypes(types);
            res.setValues(values);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * Returns new composite key with String prefix
     *
     * @param prefix - prefix for composite key
     * @param types  - list of the rest types
     * @param values - list of the rest values
     * @return composite key
     */
    public static CompositeKey compositeKey(String prefix, Class[] types, Object... values) {
        return compositeKey(CompositeKey.class,
                (Class<?>[]) ArrayUtils.addAll(new Class<?>[]{String.class}, types),
                (Object[]) ArrayUtils.addAll(new Object[]{prefix}, values));
    }

    /**
     * Create composite key using prefix
     *
     * @param prefix - prefix
     * @param values - other values
     * @return new composite key
     */
    public static CompositeKey compositeKey(String prefix, Object... values) {
        Class[] types = new Class[values.length + 1];
        for (int i = 0; i < values.length; ++i) {
            types[i] = values[i].getClass();
        }
        return compositeKey(prefix, types, values);
    }

    /**
     * Extract components of composite keys by index
     *
     * @param list    - list of composite keys
     * @param compNum - index to extract
     * @return list of components
     */
    @SuppressWarnings("unchecked")
    public static List extractFromCompositeKeys(Collection<? extends CompositeKey> list, int compNum) {
        List res = new ArrayList();
        for (CompositeKey key : list) {
            res.add(key.get(compNum));
        }
        return res;
    }

}
