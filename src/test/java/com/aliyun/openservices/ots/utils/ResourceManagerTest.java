package com.aliyun.openservices.ots.utils;

import static org.junit.Assert.*;

import java.util.Locale;

import org.junit.Test;

import com.aliyun.openservices.ots.utils.ResourceManager;

public class ResourceManagerTest {

    @Test
    public void testResourceManager(){
        // TODO: User a test-specific resource
        String baseName = "common";
        ResourceManager rm;

        Locale currentLocale = Locale.getDefault();

        try {
            Locale.setDefault(Locale.ENGLISH);
            rm = ResourceManager.getInstance(baseName);
            assertEquals("Failed to parse the response result.", rm.getString("FailedToParseResponse"));
            assertEquals("Connection error due to: test.", rm.getFormattedString("ConnectionError", "test."));

            Locale.setDefault(Locale.CHINA);
            rm = ResourceManager.getInstance(baseName);
            assertEquals("返回结果无效，无法解析。", rm.getString("FailedToParseResponse"));
            assertEquals("网络连接错误，详细信息：测试。", rm.getFormattedString("ConnectionError", "测试。"));
        } finally {
            Locale.setDefault(currentLocale);
        }
    }
}
