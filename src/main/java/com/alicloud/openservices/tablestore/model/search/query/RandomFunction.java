package com.alicloud.openservices.tablestore.model.search.query;

/**
 * 在{@link FunctionsScoreQuery}中使用，该函数可以为文档随机打分，返回随机的排序序列，每次返回结果不同
 */
public class RandomFunction {
    public RandomFunction() {}
    public static RandomFunction.Builder newBuilder() {
        return new RandomFunction.Builder();
    }

    public static final class Builder{
        private Builder() {}

        public RandomFunction build() {
            return new RandomFunction();
        }
    }

}
