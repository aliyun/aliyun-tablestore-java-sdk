/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.smoketest;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.aliyun.openservices.ots.utils.DateUtil;

import static com.aliyun.openservices.ots.utils.HttpUtil.*;

public class MTThread {
    static class Test implements Runnable {

        @Override
        public void run() {
            // while (true) {
            System.out.println("Start");
            try {
                for (int i = 0; i < 10; i++) {
                    Map<String, String> headers = new HashMap<String, String>();
                    headers.put("Hello1", "World");
                    headers.put("Hello2", "World");
                    headers.put("Hello3", "World");
                    headers.put("Hello4", "World");
                    headers.put("Hello5", "World");
                    headers.put("Hello6", "World");
                    headers.put("Hello7", "World");
                    headers.put("Hello8", "World");
                    headers.put("Hello9", "World");
                    headers.put("Hello10", "World");
                    
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("End");
        }
        // }

    }

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {
        long total = 1000000;
        long time = 8903;
        double qps = 1.0 * total / (time * 1.0 / 1000.0);
        System.out.println(qps);
        
    }
}
