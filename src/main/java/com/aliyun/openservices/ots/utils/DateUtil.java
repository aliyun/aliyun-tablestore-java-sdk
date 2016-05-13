package com.aliyun.openservices.ots.utils;

import java.util.Date;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.tz.FixedDateTimeZone;

public class DateUtil {
    // RFC 822 Date Format
    private static final String RFC822_DATE_FORMAT =
            "EEE, dd MMM yyyy HH:mm:ss z";

    // ISO 8601 format
    private static final String ISO8601_DATE_FORMAT =
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    // Alternate ISO 8601 format without fractional seconds
    private static final String ALTERNATIVE_ISO8601_DATE_FORMAT =
            "yyyy-MM-dd'T'HH:mm:ss'Z'";
    
    private static final DateTimeFormatter RFC822_DATE_FORMATTER = DateTimeFormat
            .forPattern(RFC822_DATE_FORMAT)
            .withZone(new FixedDateTimeZone("GMT", "GMT", 0, 0))
            .withLocale(Locale.US);

    private static final DateTimeFormatter ISO8601_DATE_FORMATTER = DateTimeFormat
            .forPattern(ISO8601_DATE_FORMAT)
            .withZone(DateTimeZone.UTC)
            .withLocale(Locale.US);

    private static final DateTimeFormatter ALTERNATIVE_ISO8601_DATE_FORMATTER = DateTimeFormat
            .forPattern(ALTERNATIVE_ISO8601_DATE_FORMAT)
            .withZone(DateTimeZone.UTC)
            .withLocale(Locale.US);
    
    private static volatile long currentTime = System.currentTimeMillis();
    private static volatile String currentRfc822Date = RFC822_DATE_FORMATTER.print(currentTime);
    private static AtomicBoolean isUpdating = new AtomicBoolean(false);
    
    public static String getCurrentRfc822Date() {
        long current = System.currentTimeMillis();
        if (current > currentTime + 1000) {
            if (isUpdating.compareAndSet(false, true)) {
                if (current > currentTime + 1000) {
                    currentRfc822Date = RFC822_DATE_FORMATTER.print(current);
                    currentTime = current;
                }
                isUpdating.compareAndSet(true, false);
            }
        }
        /*
         * 可能currentTime已经很旧了(10s之前)，而当前线程又没抢到锁，
         * 则强制更新。
         */
        if (current > currentTime + 10000) {
            currentRfc822Date = RFC822_DATE_FORMATTER.print(current);
            currentTime = current;
        }
        return currentRfc822Date;
    }
    
    public static String formatRfc822Date(Date date) {
        return RFC822_DATE_FORMATTER.print(date.getTime());
    }

    public static Date parseRfc822Date(String dateString) {
        return RFC822_DATE_FORMATTER.parseDateTime(dateString).toDate();
    }

    public static String formatIso8601Date(Date date) {
        return ISO8601_DATE_FORMATTER.print(date.getTime());
    }
    
    public static Date parseIso8601Date(String dateString) {
        return ISO8601_DATE_FORMATTER.parseDateTime(dateString).toDate();
    }

    public static String formatAlternativeIso8601Date(Date date) {
        return ALTERNATIVE_ISO8601_DATE_FORMATTER.print(date.getTime());
    }

    public static Date parseAlternativeIso8601Date(String dateString) {
        return ALTERNATIVE_ISO8601_DATE_FORMATTER.parseDateTime(dateString).toDate();
    }
}
