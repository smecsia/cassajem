package me.smecsia.cassajaem.meta;

import me.smecsia.cassajaem.api.CompositeKey;

import static me.smecsia.cassajaem.util.TypesUtil.compositeKey;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
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
