package me.smecsia.cassajem.meta.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation. Maps field  to a column (for standard column family or super columns sub column).
 * User: isadykov
 * Date: 25.01.12
 * Time: 19:44
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    public String name() default "";
}
