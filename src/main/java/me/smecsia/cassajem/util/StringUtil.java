package me.smecsia.cassajem.util;

/**
 * String utility class
 * User: smecsia
 * Date: 16.02.12
 * Time: 20:37
 */
public class StringUtil {

    /**
     * Returns the maximum possible string
     *
     * @return string
     */
    public static String maxString() {
        return Character.toString(Character.MAX_VALUE);
    }

    /**
     * Returns the minimum possible string
     *
     * @return string
     */
    public static String minString() {
        return Character.toString(Character.MIN_VALUE);
    }

    /**
     * Format string
     *
     * @param string string to be formatted
     * @param format format
     * @return formatted string
     */
    public static String format(String string, String format) {
        return string;
    }
}
