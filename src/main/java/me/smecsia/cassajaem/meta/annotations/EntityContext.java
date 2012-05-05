package me.smecsia.cassajaem.meta.annotations;

import me.smecsia.cassajaem.api.BasicEntity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to indicate which type of entity is used in the class
 * User: isadykov
 * Date: 06.02.12
 * Time: 12:39
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface EntityContext {
    public Class<? extends BasicEntity> entityClass() default BasicEntity.class;
}
