package com.alicloud.openservices.tablestore.timestream.model.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation, can be used for the class fields of the object in {@link com.alicloud.openservices.tablestore.timestream.model.Point.Builder#from(Object)}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Field {
    String name();
}
