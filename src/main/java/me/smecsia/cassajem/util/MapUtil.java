package me.smecsia.cassajem.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Util class to manipulate with maps
 * User: isadykov
 * Date: 11.03.12
 * Time: 17:02
 */
public class MapUtil {

    /**
     * Returns map which does not contain the keys that are not presented in includeKeys collection.
     * Returns the source map if includeKeys is null
     *
     * @param map         incoming map
     * @param includeKeys keys to include list
     * @param <K>         first map arg
     * @param <V>         second map arg
     * @return updated map
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> includeKeys(Map<K, V> map, K... includeKeys) {
        if (includeKeys == null) {
            return map;
        }
        Collection<K> keysList = Arrays.asList(includeKeys);
        Map<K, V> res = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (keysList.contains(entry.getKey())) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

}
