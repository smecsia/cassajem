package me.smecsia.cassajem.meta.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Maps class field to a list of super columns.
 * User: smecsia
 * Date: 25.01.12
 * Time: 19:44
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SuperColumnArray {
    public String name() default "";
}
