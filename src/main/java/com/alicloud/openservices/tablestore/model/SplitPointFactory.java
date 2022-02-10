package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class SplitPointFactory{

    private static int DEFAULT_MAX_LENGTH = 8;

    public static List<PrimaryKeyValue> getDigit(int pointCount, long begin, long end) {
        Preconditions.checkArgument(pointCount > 0,
                "The number of pointCount must be greater than 0.");
        List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
        end = end + 1;

        double range = (double)(end - begin) / (pointCount + 1);
        double point = begin;
        Preconditions.checkArgument(range >= 1,
                String.format("When you try to set the split points for the interval [%d, %d], you can set up to %d split points, currently %d.",
                        begin, end - 1, end - begin - 1, pointCount));

        for (long i = 0; i < pointCount; i++) {
            point += range;
            points.add(PrimaryKeyValue.fromLong((long)Math.floor(point)));
        }

        return points;
    }

    private static List<String> getHexValue(int pointCount, int pointLength){

        Preconditions.checkArgument(pointCount > 0 && pointLength > 0,
                "The number of pointCount and pointLength must be greater than 0.");

        List<String> pointsString = new ArrayList<String>();

        StringBuilder endStringBuilder = new StringBuilder();
        for (int i = 0; i < pointLength; i++){
            endStringBuilder.append('f');
        }
        String endString = endStringBuilder.toString();
        BigInteger end = new BigInteger(endString, 16);
        BigInteger begin = new BigInteger("0", 16);
        BigInteger partitionCount = new BigInteger(Integer.toString(pointCount+1));

        BigInteger range = end.subtract(begin).add(BigInteger.ONE).divide(new BigInteger(new Integer(pointCount+1).toString()));
        Preconditions.checkArgument(range != BigInteger.ZERO,
                String.format("When the length of MD5 is %d, you can set up to %d split points, currently %d.",
                        pointLength, (1 << (pointLength*4)) - 1, pointCount));
        for (int i = 0; i < pointCount; i++){
            BigInteger point = end.subtract(begin).add(BigInteger.ONE).multiply(new BigInteger(new Integer(i+1).toString()));
            point = point.divide(partitionCount);
            StringBuilder pointString = new StringBuilder().append(point.toString(16));
            while (pointLength - pointString.length() > 0){
                pointString.insert(0,'0');
            }
            pointsString.add(pointString.toString());
        }

        return pointsString;
    }

    public static List<PrimaryKeyValue> getLowerHexString(int pointCount, int pointLength){

        List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
        List<String> pointsString = getHexValue(pointCount,pointLength);

        for (int i = 0; i < pointCount; i++){
            points.add(PrimaryKeyValue.fromString(pointsString.get(i)));
        }

        return points;
    }

    public static List<PrimaryKeyValue> getUpperHexString(int pointCount, int pointLength){

        List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
        List<String> pointsString = getHexValue(pointCount,pointLength);

        for (int i = 0; i < pointCount; i++){
            points.add(PrimaryKeyValue.fromString(pointsString.get(i).toUpperCase()));
        }

        return points;
    }
}
