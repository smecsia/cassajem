package me.smecsia.cassajem.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Date util class
 * User: isadykov
 * Date: 18.01.12
 * Time: 19:49
 */
public class DateUtil {

    /**
     * Get timestamp for a date
     * 
     * @param date date
     * @return timestamp
     */
    public static Long timestamp(Date date) {
        return date.getTime();
    }

    /**
     * Get current timestamp
     * 
     * @return timestamp
     */
    public static Long timestamp() {
        return timestamp(now());
    }

    /**
     * Get string representation of current timestmap
     * 
     * @return timestamp string
     */
    public static String timestampStr() {
        return timestampStr(new Date());
    }

    /**
     * Get string representation of timestamp for a date
     *
     * @param date date
     * @return timestamp string
     */
    public static String timestampStr(Date date) {
        return String.valueOf(date.getTime());// + "." + System.nanoTime();
    }

    /**
     * Get date for calendar difference from current date
     * 
     * @param field difference field (Calendar.DATE, Calendar.MINUTE ... etc)
     * @param amount how much?
     * @return date for the difference
     */
    public static Date forCalendarDiff(int field, int amount) {
        GregorianCalendar gCal = new GregorianCalendar();
        gCal.add(field, amount);
        return new Date(gCal.getTimeInMillis());
    }

    /**
     * Return date for 30 minutes later date from now
     * 
     * @return date
     */
    public static Date thirtyMinutesLater() {
        return forCalendarDiff(Calendar.MINUTE, 30);
    }

    /**
     * Get date for 30 minutes ago date from now
     * 
     * @return date
     */
    public static Date thirtyMinutesAgo() {
        return forCalendarDiff(Calendar.MINUTE, -30);
    }

    /**
     * Get date for tomorrow (1 day from now)
     * 
     * @return date
     */
    public static Date tomorrow() {
        return forCalendarDiff(Calendar.DATE, 1);
    }

    /**
     * Get date for now
     * 
     * @return date
     */
    public static Date now() {
        return forCalendarDiff(Calendar.SECOND, 0);
    }

    /**
     * Get date for hour later from now
     * 
     * @return date
     */
    public static Date hourLater() {
        return forCalendarDiff(Calendar.HOUR, 1);
    }

    /**
     * Get date for hour ago from now
     * 
     * @return date
     */
    public static Date hourAgo() {
        return forCalendarDiff(Calendar.HOUR, -1);
    }

    /**
     * Get date for yesterday (1 day earlier than now)
     * 
     * @return date
     */
    public static Date yesterday() {
        return forCalendarDiff(Calendar.DATE, -1);
    }

    /**
     * Get date for 1 minute later (from now)
     * 
     * @return date
     */
    public static Date minuteLater() {
        return forCalendarDiff(Calendar.MINUTE, 1);
    }

    /**
     * Get date for minute ago from now
     * 
     * @return date
     */
    public static Date minuteAgo() {
        return forCalendarDiff(Calendar.MINUTE, -1);
    }

    /**
     * Get date for number of seconds later
     * 
     * @param seconds number of seconds
     * @return date
     */
    public static Date secondsLater(Integer seconds) {
        return forCalendarDiff(Calendar.SECOND, seconds);
    }

    /**
     * Get date earlier to now for number of seconds
     * 
     * @param seconds number of seconds
     * @return date
     */
    public static Date secondsAgo(Integer seconds) {
        return forCalendarDiff(Calendar.SECOND, -seconds);
    }

}
