package org.iconsider.utils;


import java.util.Calendar;

/**
 * Created by liuzhenxing on 2017-9-7.
 */
public class DateUtils {
    private static Calendar calendar;

    /**
     * 当
     * xx:05:00,
     * xx:10:00,
     * xx:15:00,
     * xx:20:00,
     * xx:25:00，
     * xx:30:00 等等的时候
     * 返回ture，否则返回false
     */
    public static boolean isMod5Min() {
        calendar = Calendar.getInstance();
        int minute = calendar.get(Calendar.MINUTE);
        int sectond = calendar.get(Calendar.SECOND);

        if(minute%5 == 0 && sectond == 0) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isMod20Min() {
        calendar = Calendar.getInstance();
        int minute = calendar.get(Calendar.MINUTE);
        int sectond = calendar.get(Calendar.SECOND);

        if(minute%20 == 0 && sectond == 0) {
            return true;
        } else {
            return false;
        }
    }

}
