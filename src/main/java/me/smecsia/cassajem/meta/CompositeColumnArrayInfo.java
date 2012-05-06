package me.smecsia.cassajem.meta;

import me.smecsia.cassajem.api.CompositeKey;

import static me.smecsia.cassajem.util.TypesUtil.compositeKey;

/**
 * Created by IntelliJ IDEA.
 * User: smecsia
 * Date: 27.01.12
 * Time: 13:53
 */
public class CompositeColumnArrayInfo extends ColumnArrayInfo {

    private Class<CompositeKey> compositeClass = CompositeKey.class;

    public CompositeColumnArrayInfo(Class<CompositeKey> compositeClass) {
        this.compositeClass = compositeClass;
    }


    public CompositeKey create(Class[] types, Object[] values) {
        return compositeKey(compositeClass, types, values);
    }

    public void setCompositeClass(Class<CompositeKey> compositeClass) {
        this.compositeClass = compositeClass;
    }
}
