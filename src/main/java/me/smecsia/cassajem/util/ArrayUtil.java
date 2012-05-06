package me.smecsia.cassajem.util;

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Array utility adapter
 * User: isadykov
 * Date: 3/30/12
 * Time: 2:33 PM
 */
public class ArrayUtil {
    private static String[] emptyStringArray = new String[0];

    /**
     * Add strings into the string array
     *
     * @param arr     array
     * @param strings strings to add
     * @return concatenated array
     */
    public static String[] add(String[] arr, String... strings) {
        return (String[]) ArrayUtils.addAll(arr, strings);
    }

    /**
     * Concatenate two arrays
     *
     * @param first  first array
     * @param second second array
     * @param <T>    type of an array
     * @return concatenated array
     */
    public static <T> T[] concat(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /**
     * Return empty string array
     *
     * @return string array
     */
    public static String[] emptyStringArray() {
        return emptyStringArray;
    }
}
