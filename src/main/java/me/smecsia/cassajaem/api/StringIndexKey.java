package me.smecsia.cassajaem.api;

import java.util.UUID;

/**
 * Composite column name that uses the following order of the composite types: &lt;UTF8:UUID:UTF8&gt;
 * Example: &lt;"pagesByVisitAndBrowserPageID":{visitUUID}:"{browserPageID}&gt;
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:43
 */
public class StringIndexKey extends CompositeKey {
    public static final Class[] keyTypes = new Class[]{String.class, UUID.class, String.class};

    public StringIndexKey() {
        super(keyTypes);
    }

    public StringIndexKey(String prefix, UUID someId, String postfix) {
        this();
        setValues(prefix, someId, postfix);
    }

    public String getPrefix() {
        return (String) get(0);
    }

    public UUID getId() {
        return (UUID) get(1);
    }

    public String getPostfix() {
        return (String) get(2);
    }

}
