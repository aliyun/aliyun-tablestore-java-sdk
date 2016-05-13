package com.aliyun.openservices.ots.internal;

import java.io.InputStream;
import java.util.Map;
import java.util.zip.Inflater;

import com.aliyun.openservices.ots.comm.ResponseHandler;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.ServiceException;
import org.apache.http.entity.ByteArrayEntity;

import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.*;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;

/**
 * 检查从OTS服务返回的响应数据是否经过压缩， 若是则进行解压
 */
public class OTSDeflateResponseHandler implements ResponseHandler{
    private final static String OTS_COMPRESS_TYPE = "deflate";

    @Override
    public void handle(ResponseMessage responseData) throws ServiceException,
            ClientException {
        Map<String, String> header = responseData.getHeadersMap();
        String compressType = header.get(OTS_HEADER_RESPONSE_COMPRESS_TYPE);
        if(compressType != null) {
            try{
                if(!OTS_COMPRESS_TYPE.equalsIgnoreCase(compressType.trim())){
                    throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidResponseCompressType", compressType.trim()));
                }
                String strRawDataSize = header.get(OTS_HEADER_RESPONSE_COMPRESS_SIZE);
                if(strRawDataSize == null){
                    throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("RequiredHeaderNotFound", OTS_HEADER_RESPONSE_COMPRESS_SIZE));
                }
                int rawDataSize = 0;
                try {
                    rawDataSize = Integer.valueOf(strRawDataSize);
                    if(rawDataSize <= 0){
                        throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidResponseCompressSize", strRawDataSize));
                    }
                } catch(NumberFormatException e) {
                    throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidResponseCompressSize", strRawDataSize));
                }
                InputStream oldInput = responseData.getContent();
                Inflater decompresser = new Inflater();
                byte[] content = OTSCompressUtil.decompress(oldInput, rawDataSize, decompresser);
                responseData.getResponse().setEntity(new ByteArrayEntity(content));
                oldInput.close();
            } catch(Exception e) {
                throw new ClientException(OTS_RESOURCE_MANAGER.getFormattedString("ResponseDecompressFail", e.getMessage()));
            }
        }
        else {
            // no need decompress, do nothing
        }
    }
    
   
}
