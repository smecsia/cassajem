package me.smecsia.cassajem.meta;

import me.prettyprint.hector.api.Serializer;
import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.util.TypesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Column information cache. Basic column information.
 * Created by IntelliJ IDEA.
 * User: smecsia
 * Date: 27.01.12
 * Time: 13:53
 */
public class ColumnInfo implements Cloneable {
    public String fieldName;
    public Class type;
    public Method getter;
    public Method setter;
    public Field field;
    public Serializer keySerializer = TypesUtil.stringSerializer;
    public Serializer valueSerializer = TypesUtil.stringSerializer;
    public Class[] compositeTypes = {};
    public boolean persist = true;

    Logger logger = LoggerFactory.getLogger(getClass());

    public Serializer getKeySerializer() {
        return keySerializer;
    }

    public boolean isComposite() {
        return compositeTypes.length > 0;
    }

    public void setKeySerializer(Serializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
        field.setAccessible(true);
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Class getType() {
        return type;
    }

    public void setType(Class type) {
        this.type = type;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }

    public Serializer getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(Serializer valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public Class[] compositeTypes() {
        return compositeTypes;
    }

    public void setCompositeTypes(Class[] compositeTypes) {
        this.compositeTypes = compositeTypes;
    }

    public Object invokeGetter(BasicEntity obj) {
        if (getter != null) {
            try {
                return getter.invoke(obj);
            } catch (Exception e) {
                logger.error("Metadata error! Cannot invoke getter for field " + getFieldName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            try {
                return getField().get(obj);
            } catch (Exception e) {
                logger.error("Metadata error! Cannot access a field " + getFieldName() + " in class " + obj.getClass().getName() + ": " + e.getMessage());
                e.printStackTrace();
            }

        }
        return null;
    }

    public void invokeSetter(BasicEntity obj, Object value) {
        if (setter != null) {
            try {
                setter.invoke(obj, value);
            } catch (Exception e) {
                logger.error("Metadata error! Cannot invoke field " + getFieldName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            try {
                getField().set(obj, value);
            } catch (Exception e) {
                logger.error("Metadata error! Cannot access a field " + getFieldName() + " in class " + obj.getClass().getName() + ": " + e.getMessage());
                e.printStackTrace();
            }

        }
    }

    @Override
    public ColumnInfo clone() {
        ColumnInfo other = new ColumnInfo();
        other.setFieldName(getFieldName());
        other.setType(getType());
        other.setGetter(getGetter());
        other.setSetter(getSetter());
        other.setCompositeTypes(compositeTypes());
        other.setKeySerializer(getKeySerializer());
        other.setValueSerializer(getValueSerializer());
        return other;
    }

}
