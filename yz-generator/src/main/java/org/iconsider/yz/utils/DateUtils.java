package org.iconsider.yz.utils;


import java.util.Calendar;

/**
 * Created by liuzhenxing on 2017-9-7.
 */
public class DateUtils {
    private static Calendar calendar;

    public static boolean isMod20Min() {
        calendar = Calendar.getInstance();
        int minute = calendar.get(Calendar.MINUTE);
        int sectond = calendar.get(Calendar.SECOND);

        if (minute % 20 == 0 && sectond == 0) {
            return true;
        } else {
            return false;
        }
    }
}
