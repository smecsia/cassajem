package me.smecsia.cassajem.meta;

import me.smecsia.cassajem.api.BasicEntity;

import java.util.HashMap;
import java.util.Map;

/**
 * Singleton allowing to cache meta info from the entities (to not read the metadata each time it is necessary)
 * User: isadykov
 * Date: 22.02.12
 * Time: 14:51
 */
public class MetaCache {
    private Map<Class<? extends BasicEntity>, Metadata<? extends BasicEntity>> cache = new HashMap<Class<? extends BasicEntity>, Metadata<? extends BasicEntity>>();
    private static MetaCache instance = null;

    private MetaCache() {
    }

    public static synchronized MetaCache instance() {
        if (instance == null) {
            instance = new MetaCache();
        }
        return instance;
    }

    /**
     * Get metadata for the class using cache
     *
     * @param clazz - class to get info (entity type class)
     * @param <T>   - entity type
     * @return Metadata for the given entity type
     */
    @SuppressWarnings("unchecked")
    public <T extends BasicEntity> Metadata<T> forClass(Class<T> clazz) {
        if (!cache.containsKey(clazz)) {
            cache.put(clazz, new Metadata<T>(clazz));
        }
        return (Metadata<T>) cache.get(clazz);
    }
}
