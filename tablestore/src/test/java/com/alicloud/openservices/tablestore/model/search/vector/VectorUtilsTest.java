package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class VectorUtilsTest extends BaseSearchTest {

    private static final Random random = new Random();

    private static float[] generateRandomFloats(int numFloats, float lowerBound, float upperBound) {
        float[] randomFloats = new float[numFloats];

        for (int i = 0; i < numFloats; i++) {
            // Generate a random floating-point number in the range [lowerBound, upperBound).
            randomFloats[i] = lowerBound + random.nextFloat() * (upperBound - lowerBound);
        }

        return randomFloats;
    }

    @Test
    public void testToBytes() {
        // Normal case
        {
            float[] vector = generateRandomFloats(1 + random.nextInt(100), 1, 100);
            byte[] bytes = VectorUtils.toBytes(vector);
            float[] newVector = VectorUtils.toFloats(bytes);
            Assert.assertEquals(vector.length, newVector.length);
            for (int i = 0; i < vector.length; i++) {
                Assert.assertEquals(vector[i], newVector[i], 0.0000001);
            }
        }
        // Exception case
        {
            float[] vector = new float[0];
            TestUtil.expectThrowsAndMessages(ClientException.class, () -> VectorUtils.toBytes(vector), "vector is null or empty");
        }
    }

    @Test
    public void testToFloats() {
        // Normal case
        {
            float[] correctVector = new float[] { 1.1f, 2.22f, 3.333f, 4.4444f };
            byte[] bytes = new byte[] { -51, -52, -116, 63, 123, 20, 14, 64, -33, 79, 85, 64, -122, 56, -114, 64 };
            float[] newVector = VectorUtils.toFloats(bytes);
            Assert.assertArrayEquals(correctVector, newVector, 0.0000001f);
        }
        // Exception case: Missing bytes
        {
            byte[] bytes = new byte[] { -51, -52, -116, 63, 123, 20, 14, 64, -33, 79, 85, 64, -122, 56 };
            TestUtil.expectThrowsAndMessages(ClientException.class, () -> VectorUtils.toFloats(bytes), "ytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0");
        }
        // Zero byte
        {
            byte[] bytes = new byte[0];
            TestUtil.expectThrowsAndMessages(ClientException.class, () -> VectorUtils.toFloats(bytes), "ytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0");
        }
    }

    @Test
    public void testFloatsAndBytesConversion() {
        int dimension = 100 + random.nextInt(924);
        float[] vector = generateRandomFloats(dimension, 0, 1);
        byte[] bytes = VectorUtils.toBytes(vector);
        float[] newVector = VectorUtils.toFloats(bytes);
        Assert.assertArrayEquals(vector, newVector, 0.0000001f);
    }
}
