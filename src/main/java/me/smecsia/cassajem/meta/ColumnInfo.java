package me.smecsia.cassajem.meta;

import me.prettyprint.hector.api.Serializer;
import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.util.TypesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Column information cache. Basic column information.
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 27.01.12
 * Time: 13:53
 */
public class ColumnInfo implements Cloneable {
    public String name;
    public Class type;
    public Method getter;
    public Method setter;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        try {
            return getter.invoke(obj);
        } catch (Exception e) {
            logger.error("Metadata error! Cannot invoke getter for field " + getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public void invokeSetter(BasicEntity obj, Object value) {
        try {
            setter.invoke(obj, value);
        } catch (Exception e) {
            logger.error("Metadata error! Cannot invoke field " + getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public ColumnInfo clone() {
        ColumnInfo other = new ColumnInfo();
        other.setName(getName());
        other.setType(getType());
        other.setGetter(getGetter());
        other.setSetter(getSetter());
        other.setCompositeTypes(compositeTypes());
        other.setKeySerializer(getKeySerializer());
        other.setValueSerializer(getValueSerializer());
        return other;
    }

}
