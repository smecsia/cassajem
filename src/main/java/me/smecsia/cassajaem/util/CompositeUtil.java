package me.smecsia.cassajaem.util;

import me.smecsia.cassajaem.util.ArrayUtil;
import org.apache.commons.lang.StringUtils;

/**
 * Utility class which helps to ease the use of composite and concatenated keys
 *
 *
 * Date: 4/11/12
 * Time: 5:02 PM
 *
 */
public class CompositeUtil {
    private static final char CONCAT_SEP_CHAR = '#';
    private static final char CATEG_SEP_CHAR = ',';

    /**
     * Creates concatenated key
     *
     * @param parts parts of a key
     * @return concatenated key
     */
    public static String ctKey(String... parts) {
        return StringUtils.join(parts, CONCAT_SEP_CHAR);
    }

    /**
     * Extract categories from the list (Ex: "HD,TV" --> String[2]{"HD", "TV"})
     * @param categories concatenated list of categories
     * @return list of categories
     */
    public static String[] extCategories(String categories) {
        if (categories == null) {
            return ArrayUtil.emptyStringArray();
        }
        return categories.split(String.valueOf(CATEG_SEP_CHAR));
    }
}
