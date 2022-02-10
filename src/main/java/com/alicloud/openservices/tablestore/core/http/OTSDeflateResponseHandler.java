package com.alicloud.openservices.tablestore.core.http;

import java.io.InputStream;
import java.util.Map;
import java.util.zip.Inflater;

import org.apache.http.entity.ByteArrayEntity;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.utils.CompressUtil;

/**
 * 检查从OTS服务返回的响应数据是否经过压缩， 若是则进行解压
 */
public class OTSDeflateResponseHandler implements ResponseHandler {
    private final static String OTS_COMPRESS_TYPE = "deflate";

    @Override
    public void handle(ResponseMessage responseData)
        throws TableStoreException, ClientException
    {
        Map<String, String> header = responseData.getLowerCaseHeadersMap();
        String compressType = header.get(Constants.OTS_HEADER_RESPONSE_COMPRESS_TYPE);
        if(compressType != null) {
            try{
                if(!OTS_COMPRESS_TYPE.equalsIgnoreCase(compressType.trim())){
                    throw new ClientException("Unsupported compress type: " + compressType);
                }
                String strRawDataSize = header.get(Constants.OTS_HEADER_RESPONSE_COMPRESS_SIZE);
                if(strRawDataSize == null){
                    throw new ClientException("Required header is not found: " + Constants.OTS_HEADER_RESPONSE_COMPRESS_SIZE);
                }
                int rawDataSize = 0;
                try {
                    rawDataSize = Integer.valueOf(strRawDataSize);
                    if(rawDataSize <= 0){
                        throw new ClientException("The compress size is invalid: " + rawDataSize);
                    }
                } catch(NumberFormatException e) {
                    throw new ClientException("The compress size is invalid: " + rawDataSize);
                }
                InputStream oldInput = responseData.getContent();
                Inflater decompresser = new Inflater();
                byte[] content = CompressUtil.decompress(oldInput, rawDataSize, decompresser);
                responseData.getResponse().setEntity(new ByteArrayEntity(content));
                oldInput.close();
            } catch(Exception e) {
                throw new ClientException("Decompress response failed.", e);
            }
        }
        else {
            // no need decompress, do nothing
        }
    }
    
   
}
