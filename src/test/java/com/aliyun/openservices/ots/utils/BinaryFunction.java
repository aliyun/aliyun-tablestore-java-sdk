package com.aliyun.openservices.ots.utils;

public interface BinaryFunction <TArg1, TArg2, TResult>{
    TResult evaluate(TArg1 arg1, TArg2 arg2);
}
