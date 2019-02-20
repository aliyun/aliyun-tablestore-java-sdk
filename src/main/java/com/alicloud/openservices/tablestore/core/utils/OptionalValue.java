package com.alicloud.openservices.tablestore.core.utils;

public class OptionalValue<T> {

    private String name;
    private T value;
    private boolean isSet;

    public OptionalValue(String name) {
        this.name = name;
        this.value = null;
        this.isSet = false;
    }

    public void setValue(T value) {
        isSet = true;
        this.value = value;
    }

    public T getValue() {
        if (!isSet) {
            return null;
        }
        return value;
    }

    public boolean isValueSet() {
        return isSet;
    }

    public void clear() {
        this.value = null;
        this.isSet = false;
    }

    @Override
    public int hashCode() {
        return isSet ? value.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof OptionalValue)) {
            return false;
        }

        OptionalValue v1 = (OptionalValue) o;
        if (this.isSet) {
            return v1.isSet ? value.equals(v1.value) : false;
        } else {
            return !v1.isSet ? true : false;
        }
    }

    @Override
    public String toString() {
        if (isSet) {
            return name + ":" + value;
        } else {
            return name + ":NotSet";
        }
    }
}
