package com.alicloud.openservices.tablestore.timestream.model.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 注解，可以用于{@link com.alicloud.openservices.tablestore.timestream.model.Point.Builder#from(Object)}中object的类字段
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Field {
    String name();
}
