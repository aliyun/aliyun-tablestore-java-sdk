package com.alicloud.openservices.tablestore.model;

public class RequestExtension {
   private Priority priority;
   private String tag;

   public void setPriority(Priority priority) {this.priority = priority;}
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
   public Priority getPriority() {return priority;}
   public String getTag() {return tag;}
}
