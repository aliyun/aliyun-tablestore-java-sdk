package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.ClientException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VectorUtils {
    private static final ByteOrder order = ByteOrder.LITTLE_ENDIAN;

    /**
     * Serialize a float array to a byte array
     * @param vector float array
     * @return byte array
     */
    public static byte[] toBytes(float[] vector) {
        if (vector == null || vector.length == 0) {
            throw new ClientException("vector is null or empty");
        }
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * 4);
        buffer.order(order);
        for (float value : vector) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }

    /**
     * Deserializes a byte array into a float array.
     * @param bytes Byte array
     * @return Float array
     */
    public static float[] toFloats(byte[] bytes) {
        int length = bytes.length / 4;
        if (bytes.length % 4 != 0 || length == 0) {
            throw new ClientException("bytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(order);
        float[] vector = new float[length];
        buffer.asFloatBuffer().get(vector);
        return vector;
    }
}
