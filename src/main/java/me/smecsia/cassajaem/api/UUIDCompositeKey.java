package me.smecsia.cassajaem.api;

import java.util.UUID;

/**
 * Composite column name which uses the UUID as a value of a composite name: &lt;"{prefix}":{UUID}&gt;
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:43
 */
public class UUIDCompositeKey extends PrefixCompositeKey {

    public UUIDCompositeKey() {
        super();
    }

    public UUIDCompositeKey(Class<?>... types) {
        super(types);
    }

    public UUIDCompositeKey(String prefix, Class<?>[] types) {
        super(prefix, types);
    }

    public UUIDCompositeKey(String prefix, UUID value) {
        this(prefix);
        set(value);
    }

    public UUIDCompositeKey(String prefix) {
        this(prefix, new Class<?>[]{UUID.class});
    }

    public UUID getUUID() {
        return (UUID) values[values.length - 1];
    }

}
