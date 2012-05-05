package me.smecsia.cassajaem.meta.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation. Defines the field which should hold the key of the row.
 * User: isadykov
 * Date: 25.01.12
 * Time: 19:44
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
    boolean persist() default true;
    boolean useTimeUUID() default true;
    Class<?> keyValidationClass() default Object.class;
}
