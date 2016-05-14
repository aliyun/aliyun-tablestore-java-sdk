package com.aliyun.openservices.ots.utils;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

public class DateUtilUnittest {
    private Date date = new Date(1408962632777L);
    private String stringRfc822Date = "Mon, 25 Aug 2014 10:30:32 GMT";
    private String stringIso8601Date = "2014-08-25T10:30:32.777Z";
    private String stringAlternativeIso8601Date = "2014-08-25T10:30:32Z";
   
    @Test
    public void testRfc822Date() {
        assertEquals(date.getTime()/1000, DateUtil.parseRfc822Date(stringRfc822Date).getTime()/1000);
        assertEquals(stringRfc822Date, DateUtil.formatRfc822Date(date));
    }
    
    @Test
    public void testIso8601Date() {
        assertEquals(date.getTime()/1000, DateUtil.parseIso8601Date(stringIso8601Date).getTime()/1000);
        assertEquals(stringIso8601Date, DateUtil.formatIso8601Date(date));
    }

    @Test
    public void testAlternativeIso8601Date() {
        assertEquals(date.getTime()/1000, DateUtil.parseAlternativeIso8601Date(stringAlternativeIso8601Date).getTime()/1000);
        assertEquals(stringAlternativeIso8601Date, DateUtil.formatAlternativeIso8601Date(date));
    }
}
