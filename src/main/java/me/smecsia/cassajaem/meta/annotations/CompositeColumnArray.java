package me.smecsia.cassajaem.meta.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 25.01.12
 * Time: 19:44
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CompositeColumnArray {
    public String prefix() default "";
}
