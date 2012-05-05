package me.smecsia.cassajaem.meta.annotations;

import me.prettyprint.hector.api.ddl.ColumnType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Class annotation. Defines the mapping of a current class to a column family.
 * User: isadykov
 * Date: 25.01.12
 * Time: 19:40
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnFamily {
    public static enum SavePolicy {
        REWRITE,
        APPEND
    }

    public boolean useTimeForUUIDs() default true;

    public SavePolicy savePolicy() default SavePolicy.REWRITE;

    public String name();

    public ColumnType columnType() default ColumnType.STANDARD;

    public String dataStorageSuperColumn() default "";

    public Class[] compositeColumnTypes() default {};
}
