package me.smecsia.cassajem.util;

import me.smecsia.cassajem.api.Conditions;

/**
 * Class to ease conditions creation
 * User: smecsia
 * Date: 13.05.12
 * Time: 0:05
 */
public class ConditionsUtil {

    /**
     * Add equals condition
     *
     * @param propName  property
     * @param propValue value
     * @return updated conditions object
     */
    public static Conditions eq(String propName, String propValue){
        return Conditions.cond().eq(propName, propValue);
    }

    /**
     * Add lesser or equals condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions lteq(Object prop, Object value) {
        return Conditions.cond().lteq(prop, value);
    }

    /**
     * Add lesser condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions lt(Object prop, Object value) {
        return Conditions.cond().lt(prop, value);
    }

    /**
     * Add greater condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions gt(Object prop, Object value) {
        return Conditions.cond().gt(prop, value);
    }

    /**
     * Add greater or equals condition
     *
     * @param prop  property
     * @param value value
     * @return updated conditions object
     */
    public Conditions gteq(Object prop, Object value) {
        return Conditions.cond().gteq(prop, value);
    }
}
