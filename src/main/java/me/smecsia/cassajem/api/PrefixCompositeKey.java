package me.smecsia.cassajem.api;

import org.apache.commons.lang.ArrayUtils;

/**
 * Composite key that use the prefix (Ex: &lt;"pages":{timeUUID}&gt;
 * User: smecsia
 * Date: 16.02.12
 * Time: 15:43
 */
public class PrefixCompositeKey extends CompositeKey {
    protected String prefix = "";

    public PrefixCompositeKey() {
        super(String.class);
    }

    public PrefixCompositeKey(Class<?>... types) {
        super((Class<?>[]) ArrayUtils.addAll(new Class<?>[]{String.class}, types));
    }

    public PrefixCompositeKey(String prefix, Class<?>[] types) {
        super((Class<?>[]) ArrayUtils.addAll(new Class<?>[]{String.class}, types));
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public PrefixCompositeKey set(Object... values) {
        return (PrefixCompositeKey) super.set((Object[]) ArrayUtils.addAll(new Object[]{prefix}, values));
    }


    @Override
    public String toString() {
        return values[values.length - 1].toString();
    }
}
