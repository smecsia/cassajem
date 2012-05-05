package me.smecsia.cassajaem.util;

import com.eaio.uuid.UUIDGen;

import java.util.Date;
import java.util.UUID;

import static me.smecsia.cassajaem.util.DateUtil.*;

/**
 * UUID utility class
 * UUIDUtil: isadykov
 * Date: 18.01.12
 * Time: 17:48
 */
public class UUIDUtil {

    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
    /**
     * Extract timestamp from timeUUID
     * 
     * @param timeUUID time UUID
     * @return timestamp
     */
    public static long timeFromTimeUUID(UUID timeUUID) {
        return (timeUUID.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * Get time UUID for a given date
     * 
     * @param date date
     * @return time UUID
     */
    public static UUID timeUUID(Date date) {
        return uuidForDate(date);
    }

    /**
     * Get time UUID for a given date (alias)
     * 
     * @param d date
     * @return time UUID
     */
    public static UUID uuidForDate(Date d) {
        return new java.util.UUID(createTime(d.getTime()), UUIDGen.getClockSeqAndNode());
    }


    /**
     * Get current time UUID
     *
     * @return time UUID
     */
    public static UUID timeUUID() {
        return new java.util.UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
//        return TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    }

    /**
     * Get minimal possible time UUID
     *
     * @return time UUID
     */
    public static UUID minTimeUUID() {
        return UUID.fromString("00000000-0000-1000-8000-000000000000");
    }

    /**
     * Get maximal possible time UUID
     *
     * @return time UUID
     */
    public static UUID maxTimeUUID() {
        return UUID.fromString("FFFFFFFF-FFFF-1FFF-8FFF-FFFFFFFFFFFF");
    }

    /**
     * Get time UUID 30 minutes later than now
     * 
     * @return time UUID
     */
    public static UUID uuid30MinutesLater() {
        return uuidForDate(thirtyMinutesLater());
    }

    /**
     * Get timeUUID for 30 minutes ago from now
     * 
     * @return time UUID
     */
    public static UUID uuid30MinutesAgo() {
        return uuidForDate(thirtyMinutesAgo());
    }

    /**
     * Get time UUID for tomorrow
     * 
     * @return time UUID
     */
    public static UUID uuidTomorrow() {
        return uuidForDate(tomorrow());
    }

    /**
     * Get time UUID for minute later from now
     * 
     * @return time UUID
     */
    public static UUID uuidMinuteLater() {
        return uuidForDate(minuteLater());
    }

    /**
     * Get time UUID for minute ago from now
     * 
     * @return time UUID
     */
    public static UUID uuidMinuteAgo() {
        return uuidForDate(minuteAgo());
    }

    /**
     * Get time UUID for hour later from now
     * 
     * @return time UUID
     */
    public static UUID uuidHourLater() {
        return uuidForDate(hourLater());
    }

    /**
     * Get time UUID for hour ago from now
     * 
     * @return time UUID
     */
    public static UUID uuidHourAgo() {
        return uuidForDate(hourAgo());
    }

    /**
     * Get time UUID for yesterday
     * 
     * @return time UUID
     */
    public static UUID uuidYesterday() {
        return uuidForDate(yesterday());
    }


    private static long createTime(long currentTime) {
        long time;

        // UTC time
        long timeToUse = (currentTime * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

        // time low
        time = timeToUse << 32;

        // time mid
        time |= (timeToUse & 0xFFFF00000000L) >> 16;

        // time hi and version
        time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
        return time;
    }

}
