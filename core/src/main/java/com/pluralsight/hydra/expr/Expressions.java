package com.pluralsight.hydra.expr;

import org.joda.time.DateTime;

/**
 * Created by alexsilva on 7/7/17.
 */
public class Expressions {

    public static String current_timestamp() {
        return String.valueOf(System.currentTimeMillis());
    }

    public static int this_month() {
        return DateTime.now().getMonthOfYear();
    }
}
