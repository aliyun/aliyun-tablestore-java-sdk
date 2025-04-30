package com.alicloud.openservices.tablestore.core.utils;

public final class Preconditions {
    private Preconditions() {
    }

    public static void checkArgument(boolean expression) {
        if(!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, Object errorMessage) {
        if(!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs) {
        if(!expression) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static <T> T checkNotNull(T reference) {
        if(reference == null) {
            throw new IllegalArgumentException();
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if(reference == null) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(T reference, String errorMessageTemplate, Object... errorMessageArgs) {
        if(reference == null) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        } else {
            return reference;
        }
    }
    
    public static void checkNotEmptyString(String reference, Object errorMessage) {
        if(reference.equals("")) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
    
    public static void checkStringNotNullAndEmpty(String reference, Object errorMessage) {
        if(reference == null || reference.equals("")) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    static String format(String template, Object... args) {
        template = String.valueOf(template);
        StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
        int templateStart = 0;

        int i;
        int placeholderStart;
        for(i = 0; i < args.length; templateStart = placeholderStart + 2) {
            placeholderStart = template.indexOf("%s", templateStart);
            if(placeholderStart == -1) {
                break;
            }

            builder.append(template.substring(templateStart, placeholderStart));
            builder.append(args[i++]);
        }

        builder.append(template.substring(templateStart));
        if(i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);

            while(i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }

            builder.append("]");
        }

        return builder.toString();
    }
}
