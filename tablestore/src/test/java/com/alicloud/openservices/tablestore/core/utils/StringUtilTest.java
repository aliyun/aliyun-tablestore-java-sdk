package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.core.Constants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringUtilTest {
    @Test
    public void testRegexMatch() {
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "a"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc/s"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc.s"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc_a"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc-a"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "_"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "-"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "/"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "."));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "+"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "45tzcjfdcsau3dsuo9du2k6g"));
        assertEquals(true, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "STS.NTAqTQC8MEi51BZu2MJZk3ton"));
        assertEquals(false, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc&s"));
        assertEquals(false, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, ""));
        assertEquals(false, StringUtils.matchRegexPattern(Constants.ACCESSKEYID_REGEX, "abc%"));
    }
}
