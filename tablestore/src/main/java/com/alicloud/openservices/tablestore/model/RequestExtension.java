package com.alicloud.openservices.tablestore.model;

public class RequestExtension {
    private Priority priority;
    private String tag;
    /**
     * search 请求携带标识子用户流量来源的 tag，高级选项。填写后服务端将可能根据该 tag 限流，从而避免影响其他子用户流量
     */
    private String searchTag;

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public void setTag(String tag) {
        if (!tag.matches("\\A\\p{ASCII}*\\z")) {
            throw new IllegalArgumentException("Only support ASCII: " + tag);
        } else {
            if (tag.getBytes().length > 16) {
                String t = new String(tag.getBytes(), 0, 16);
                this.tag = t;
            } else {
                this.tag = tag;
            }
        }
    }

    public void setSearchTag(String searchTag) {
        this.searchTag = searchTag;
    }

    public Priority getPriority() {
        return priority;
    }

    public String getTag() {
        return tag;
    }

    public String getSearchTag() {
        return searchTag;
    }
}
