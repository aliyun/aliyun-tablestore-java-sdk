package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.timestream.TimestreamRestrict;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;

import java.util.TreeMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;

/**
 * Unique identifier for the timeline
 */
public class TimestreamIdentifier {
    public static final class Builder {
        private String name = null;
        private Map<String, String> tags = new TreeMap<String, String>();

        public Builder(String name) {
            if (name.length() > TimestreamRestrict.NAME_LEN_BYTE) {
                throw new ClientException(String.format("The length(%s) of name larger than %d.", name.length(), TimestreamRestrict.NAME_LEN_BYTE));
            }
            this.name = name;
        }
        /**
         * The key of the tag cannot contain ", = characters
         * @param param
         */
        private void checkKeyParams(String param) {
            if (param.indexOf("=") >= 0) {
                throw new ClientException("Illegal character exist: =.");
            }
        }

        public Builder setTags(TreeMap<String, String> tags) {
            for (String key : tags.keySet()) {
                checkKeyParams(key);
            }
            this.tags = tags;
            return this;
        }

        public Builder addTag(String name, String value) {
            checkKeyParams(name);
            this.tags.put(name, value);
            return this;
        }

        public TimestreamIdentifier build() {
            TimestreamIdentifier identifier = new TimestreamIdentifier(name);
            identifier.setTags(tags);
            return identifier;
        }
    }

    private String name;
    private Map<String, String> tags;

    private TimestreamIdentifier(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    private TimestreamIdentifier setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public String getTagValue(String name) {
        return this.tags.get(name);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() * 31 + this.tags.hashCode();
    }

    private static boolean compareMap(Map<String, String> a, Map<String, String> b) {
        if (a.size() != b.size()) {
            return false;
        }
        Iterator<Entry<String, String>> iter1 = a.entrySet().iterator();
        while(iter1.hasNext()) {
            Map.Entry<String, String> entry1 = (Entry<String, String>) iter1.next();
            String val1 = entry1.getValue();
            String val2 = b.get(entry1.getKey());
            if (!val1.equals(val2)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TimestreamIdentifier)) {
            return false;
        }

        TimestreamIdentifier val = (TimestreamIdentifier) o;
        if (!this.name.equals(val.name)) {
            return false;
        }
        if (!compareMap(this.tags, val.tags)) {
            return false;
        }
        return true;
    }

    public int getDataSize() {
        int size = this.name.length();
        Iterator<Entry<String, String>> iter1 = this.tags.entrySet().iterator();
        while(iter1.hasNext()) {
            Map.Entry<String, String> entry1 = (Entry<String, String>) iter1.next();
            size += entry1.getKey().length() + entry1.getValue().length();
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name=");
        sb.append(name);
        for (String name : this.tags.keySet()) {
            sb.append(", ");
            sb.append(name);
            sb.append("=");
            sb.append(this.tags.get(name));
        }
        return sb.toString();
    }
}
