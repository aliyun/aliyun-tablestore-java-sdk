/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.PrimaryKeyType;


/**
 * Utils.
 *
 */
public class OTSUtil {

    public static final ResourceManager OTS_RESOURCE_MANAGER =
            ResourceManager.getInstance("ots");

    public static byte[] dataEncode(String data){
        try {
            return data.getBytes(OTSConsts.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("Unsupported encoding：" + OTSConsts.DEFAULT_ENCODING);
        }
    }
    
    public static String dataDecode(byte[] bytes) {
        try {
            CharsetDecoder decoder = Charset.forName(OTSConsts.DEFAULT_ENCODING).newDecoder().onMalformedInput(CodingErrorAction.REPORT);
            CharBuffer cb = decoder.decode(ByteBuffer.wrap(bytes));
            return cb.toString();
        } catch(CharacterCodingException e) {
            throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("ValueInInvalidCharset", new String(bytes)));
        }
    }
    /**
     * 参考规范： http://tools.ietf.org/html/rfc3629
     */
    public static int stringtoUTF8Bytes(String str, byte[] buffer) {
        int index = 0;
        for (int i = 0; i < str.length(); i++) {
            char strChar = str.charAt(i);
            if( (strChar & 0xFF80) == 0 ) {
                // (00000000 00000000 - 00000000 01111111) -> 0xxxxxxx
                buffer[index++] = (byte)(strChar & 0x00FF);
            } else if( (strChar & 0xF800) == 0 ) {
                // (00000000 10000000 - 00000111 11111111) -> 110xxxxx 10xxxxxx
                buffer[index++] = (byte)((strChar >> 6) | 0x00c0);
                buffer[index++] = (byte)((strChar & 0x003F) | 0x0080);
            } else {
                // (00001000 00000000 - 11111111 11111111) -> 1110xxxx 10xxxxxx 10xxxxxx
                buffer[index++] = (byte)((strChar >> 12) | 0x00e0);
                buffer[index++] = (byte)(((strChar >> 6) & 0x003F) | 0x0080);
                buffer[index++] = (byte)((strChar & 0x003F) | 0x0080);
            }
        }
        return index;
    }

    // return 0 : equals. >0 : left is greater. <0 : right is greater.
    public static int compare(PrimaryKeyValue left, PrimaryKeyValue right){
        if (left == null && right == null){
            return 0;
        }
        
        if (left == null){
            return -1;
        }
        if (right == null){
            return 1;
        }
        
        // left == InfMin
        if (left.equals(PrimaryKeyValue.INF_MIN)){
            return right.equals(PrimaryKeyValue.INF_MIN) ? 0 : -1;
        }
        
        // left == InfMax
        if (left.equals(PrimaryKeyValue.INF_MAX)){
            return right.equals(PrimaryKeyValue.INF_MAX) ? 0 : 1;
        }
        
        // right == InfMin
        if (right.equals(PrimaryKeyValue.INF_MIN)){
            return left.equals(PrimaryKeyValue.INF_MIN) ? 0 : 1;
        }
        
        // right == InfMax
        if (right.equals(PrimaryKeyValue.INF_MAX)){
            return left.equals(PrimaryKeyValue.INF_MAX) ? 0 : -1;
        }
        
        if (!( left.getType() != null && right.getType() != null
                && left.getType() == right.getType() )) {
            throw new AssertionError("type mismatch.");
        };
        if (left.getType() == PrimaryKeyType.INTEGER){
            long lLeft = left.asLong();
            long lRight = right.asLong();
            if (lLeft > lRight){
                return 1;
            } else if (lLeft < lRight){
                return -1;
            } else {
                return 0;
            }
        }
        
        if (left.getType() != PrimaryKeyType.STRING) {
            throw new AssertionError("type mismatch.");
        }
        return left.asString().compareTo(right.asString());
    }

    // Is a PrimaryKeyValue INF_MAX / INF_MIN
    public static boolean isPKInf(PrimaryKeyValue pk){
        return pk == PrimaryKeyValue.INF_MAX || pk == PrimaryKeyValue.INF_MIN;
    }
    
}
