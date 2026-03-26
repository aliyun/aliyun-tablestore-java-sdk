package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Metadata filter for knowledge base retrieval.
 * Supports various filter operators for metadata-based filtering.
 * A MetadataFilter can only contain ONE type of operator at a time.
 */
public class MetadataFilter {
    private static final Gson GSON = new Gson();
    
    // Basic comparison operators
    private EqualsCondition equals;
    private NotEqualsCondition notEquals;
    private GreaterThanCondition greaterThan;
    private GreaterThanOrEqualsCondition greaterThanOrEquals;
    private LessThanCondition lessThan;
    private LessThanOrEqualsCondition lessThanOrEquals;
    
    // List operators
    private InCondition in;
    private NotInCondition notIn;
    
    // String operators
    private StartsWithCondition startsWith;
    private StringContainsCondition stringContains;
    private ListContainsCondition listContains;
    
    // Logical operators
    private List<MetadataFilter> andAll;
    private List<MetadataFilter> orAll;

    private MetadataFilter() {
    }

    // Builder pattern for constructing filters
    public static Builder builder() {
        return new Builder();
    }

    // Static factory methods for basic operators
    public static MetadataFilter equals(String key, Object value) {
        MetadataFilter filter = new MetadataFilter();
        filter.equals = new EqualsCondition(key, value);
        return filter;
    }

    public static MetadataFilter notEquals(String key, Object value) {
        MetadataFilter filter = new MetadataFilter();
        filter.notEquals = new NotEqualsCondition(key, value);
        return filter;
    }

    public static MetadataFilter greaterThan(String key, Number value) {
        MetadataFilter filter = new MetadataFilter();
        filter.greaterThan = new GreaterThanCondition(key, value);
        return filter;
    }

    public static MetadataFilter greaterThanOrEquals(String key, Number value) {
        MetadataFilter filter = new MetadataFilter();
        filter.greaterThanOrEquals = new GreaterThanOrEqualsCondition(key, value);
        return filter;
    }

    public static MetadataFilter lessThan(String key, Number value) {
        MetadataFilter filter = new MetadataFilter();
        filter.lessThan = new LessThanCondition(key, value);
        return filter;
    }

    public static MetadataFilter lessThanOrEquals(String key, Number value) {
        MetadataFilter filter = new MetadataFilter();
        filter.lessThanOrEquals = new LessThanOrEqualsCondition(key, value);
        return filter;
    }

    public static MetadataFilter in(String key, List<String> value) {
        MetadataFilter filter = new MetadataFilter();
        filter.in = new InCondition(key, value);
        return filter;
    }

    public static MetadataFilter in(String key, String... values) {
        return in(key, Arrays.asList(values));
    }

    public static MetadataFilter notIn(String key, List<String> value) {
        MetadataFilter filter = new MetadataFilter();
        filter.notIn = new NotInCondition(key, value);
        return filter;
    }

    public static MetadataFilter notIn(String key, String... values) {
        return notIn(key, Arrays.asList(values));
    }

    public static MetadataFilter startsWith(String key, String value) {
        MetadataFilter filter = new MetadataFilter();
        filter.startsWith = new StartsWithCondition(key, value);
        return filter;
    }

    public static MetadataFilter stringContains(String key, String value) {
        MetadataFilter filter = new MetadataFilter();
        filter.stringContains = new StringContainsCondition(key, value);
        return filter;
    }

    public static MetadataFilter listContains(String key, String value) {
        MetadataFilter filter = new MetadataFilter();
        filter.listContains = new ListContainsCondition(key, value);
        return filter;
    }

    public static MetadataFilter andAll(MetadataFilter... filters) {
        return andAll(Arrays.asList(filters));
    }

    public static MetadataFilter andAll(List<MetadataFilter> filters) {
        MetadataFilter filter = new MetadataFilter();
        filter.andAll = new ArrayList<>(filters);
        return filter;
    }

    public static MetadataFilter orAll(MetadataFilter... filters) {
        return orAll(Arrays.asList(filters));
    }

    public static MetadataFilter orAll(List<MetadataFilter> filters) {
        MetadataFilter filter = new MetadataFilter();
        filter.orAll = new ArrayList<>(filters);
        return filter;
    }

    /**
     * Convert filter to JSON string
     */
    public String toJson() {
        try {
            return GSON.toJson(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize MetadataFilter to JSON", e);
        }
    }

    // Getters and Setters for basic comparison operators
    public EqualsCondition getEquals() {
        return equals;
    }

    public void setEquals(EqualsCondition equals) {
        this.equals = equals;
    }

    public NotEqualsCondition getNotEquals() {
        return notEquals;
    }

    public void setNotEquals(NotEqualsCondition notEquals) {
        this.notEquals = notEquals;
    }

    public GreaterThanCondition getGreaterThan() {
        return greaterThan;
    }

    public void setGreaterThan(GreaterThanCondition greaterThan) {
        this.greaterThan = greaterThan;
    }

    public GreaterThanOrEqualsCondition getGreaterThanOrEquals() {
        return greaterThanOrEquals;
    }

    public void setGreaterThanOrEquals(GreaterThanOrEqualsCondition greaterThanOrEquals) {
        this.greaterThanOrEquals = greaterThanOrEquals;
    }

    public LessThanCondition getLessThan() {
        return lessThan;
    }

    public void setLessThan(LessThanCondition lessThan) {
        this.lessThan = lessThan;
    }

    public LessThanOrEqualsCondition getLessThanOrEquals() {
        return lessThanOrEquals;
    }

    public void setLessThanOrEquals(LessThanOrEqualsCondition lessThanOrEquals) {
        this.lessThanOrEquals = lessThanOrEquals;
    }

    // Getters and Setters for list operators
    public InCondition getIn() {
        return in;
    }

    public void setIn(InCondition in) {
        this.in = in;
    }

    public NotInCondition getNotIn() {
        return notIn;
    }

    public void setNotIn(NotInCondition notIn) {
        this.notIn = notIn;
    }

    // Getters and Setters for string operators
    public StartsWithCondition getStartsWith() {
        return startsWith;
    }

    public void setStartsWith(StartsWithCondition startsWith) {
        this.startsWith = startsWith;
    }

    public StringContainsCondition getStringContains() {
        return stringContains;
    }

    public void setStringContains(StringContainsCondition stringContains) {
        this.stringContains = stringContains;
    }

    public ListContainsCondition getListContains() {
        return listContains;
    }

    public void setListContains(ListContainsCondition listContains) {
        this.listContains = listContains;
    }

    // Getters and Setters for logical operators
    public List<MetadataFilter> getAndAll() {
        return andAll;
    }

    public void setAndAll(List<MetadataFilter> andAll) {
        this.andAll = andAll;
    }

    public List<MetadataFilter> getOrAll() {
        return orAll;
    }

    public void setOrAll(List<MetadataFilter> orAll) {
        this.orAll = orAll;
    }

    /**
     * Builder class for constructing complex filters
     */
    public static class Builder {
        private List<MetadataFilter> filters = new ArrayList<>();

        public Builder add(MetadataFilter filter) {
            filters.add(filter);
            return this;
        }

        public Builder equals(String key, Object value) {
            filters.add(MetadataFilter.equals(key, value));
            return this;
        }

        public Builder notEquals(String key, Object value) {
            filters.add(MetadataFilter.notEquals(key, value));
            return this;
        }

        public Builder greaterThan(String key, Number value) {
            filters.add(MetadataFilter.greaterThan(key, value));
            return this;
        }

        public Builder greaterThanOrEquals(String key, Number value) {
            filters.add(MetadataFilter.greaterThanOrEquals(key, value));
            return this;
        }

        public Builder lessThan(String key, Number value) {
            filters.add(MetadataFilter.lessThan(key, value));
            return this;
        }

        public Builder lessThanOrEquals(String key, Number value) {
            filters.add(MetadataFilter.lessThanOrEquals(key, value));
            return this;
        }

        public Builder in(String key, List<String> value) {
            filters.add(MetadataFilter.in(key, value));
            return this;
        }

        public Builder in(String key, String... values) {
            filters.add(MetadataFilter.in(key, values));
            return this;
        }

        public Builder notIn(String key, List<String> value) {
            filters.add(MetadataFilter.notIn(key, value));
            return this;
        }

        public Builder notIn(String key, String... values) {
            filters.add(MetadataFilter.notIn(key, values));
            return this;
        }

        public Builder startsWith(String key, String value) {
            filters.add(MetadataFilter.startsWith(key, value));
            return this;
        }

        public Builder stringContains(String key, String value) {
            filters.add(MetadataFilter.stringContains(key, value));
            return this;
        }

        public Builder listContains(String key, String value) {
            filters.add(MetadataFilter.listContains(key, value));
            return this;
        }

        public MetadataFilter buildAnd() {
            if (filters.isEmpty()) {
                throw new IllegalStateException("Cannot build andAll filter with no conditions");
            }
            if (filters.size() == 1) {
                return filters.get(0);
            }
            return MetadataFilter.andAll(filters);
        }

        public MetadataFilter buildOr() {
            if (filters.isEmpty()) {
                throw new IllegalStateException("Cannot build orAll filter with no conditions");
            }
            if (filters.size() == 1) {
                return filters.get(0);
            }
            return MetadataFilter.orAll(filters);
        }
    }

    // Inner classes for each condition type
    
    /**
     * Equals condition: attribute value equals specified value.
     * Supports: string, number, boolean
     */
    public static class EqualsCondition {
        private String key;
        private Object value;

        public EqualsCondition() {
        }

        public EqualsCondition(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    /**
     * Not equals condition: attribute value does not equal specified value.
     * Supports: string, number, boolean
     */
    public static class NotEqualsCondition {
        private String key;
        private Object value;

        public NotEqualsCondition() {
        }

        public NotEqualsCondition(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    /**
     * Greater than condition: attribute value is greater than specified value.
     * Supports: number only
     */
    public static class GreaterThanCondition {
        private String key;
        private Number value;

        public GreaterThanCondition() {
        }

        public GreaterThanCondition(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Number getValue() {
            return value;
        }

        public void setValue(Number value) {
            this.value = value;
        }
    }

    /**
     * Greater than or equals condition: attribute value is greater than or equal to specified value.
     * Supports: number only
     */
    public static class GreaterThanOrEqualsCondition {
        private String key;
        private Number value;

        public GreaterThanOrEqualsCondition() {
        }

        public GreaterThanOrEqualsCondition(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Number getValue() {
            return value;
        }

        public void setValue(Number value) {
            this.value = value;
        }
    }

    /**
     * Less than condition: attribute value is less than specified value.
     * Supports: number only
     */
    public static class LessThanCondition {
        private String key;
        private Number value;

        public LessThanCondition() {
        }

        public LessThanCondition(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Number getValue() {
            return value;
        }

        public void setValue(Number value) {
            this.value = value;
        }
    }

    /**
     * Less than or equals condition: attribute value is less than or equal to specified value.
     * Supports: number only
     */
    public static class LessThanOrEqualsCondition {
        private String key;
        private Number value;

        public LessThanOrEqualsCondition() {
        }

        public LessThanOrEqualsCondition(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Number getValue() {
            return value;
        }

        public void setValue(Number value) {
            this.value = value;
        }
    }

    /**
     * In condition: attribute value is in the specified list.
     * Supports: string list
     */
    public static class InCondition {
        private String key;
        private List<String> value;

        public InCondition() {
        }

        public InCondition(String key, List<String> value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public List<String> getValue() {
            return value;
        }

        public void setValue(List<String> value) {
            this.value = value;
        }
    }

    /**
     * Not in condition: attribute value is not in the specified list.
     * Supports: string list
     */
    public static class NotInCondition {
        private String key;
        private List<String> value;

        public NotInCondition() {
        }

        public NotInCondition(String key, List<String> value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public List<String> getValue() {
            return value;
        }

        public void setValue(List<String> value) {
            this.value = value;
        }
    }

    /**
     * Starts with condition: attribute value starts with specified string.
     * Supports: string
     */
    public static class StartsWithCondition {
        private String key;
        private String value;

        public StartsWithCondition() {
        }

        public StartsWithCondition(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    /**
     * String contains condition: attribute value contains specified substring.
     * Supports: string
     */
    public static class StringContainsCondition {
        private String key;
        private String value;

        public StringContainsCondition() {
        }

        public StringContainsCondition(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    /**
     * List contains condition: list attribute contains specified value as a member.
     * Supports: string_list
     */
    public static class ListContainsCondition {
        private String key;
        private String value;

        public ListContainsCondition() {
        }

        public ListContainsCondition(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
