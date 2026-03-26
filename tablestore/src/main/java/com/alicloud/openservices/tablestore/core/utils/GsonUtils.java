package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.model.knowledgebase.DocumentStatus;
import com.alicloud.openservices.tablestore.model.knowledgebase.RerankingType;
import com.alicloud.openservices.tablestore.model.knowledgebase.SearchType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.ToNumberPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Utility class for JSON serialization and deserialization using Gson.
 * <p>
 * This class provides a shared, thread-safe Gson instance with standardized configuration for the entire TableStore SDK. All model classes should use this
 * utility instead of creating their own Gson instances.
 * </p>
 */
public final class GsonUtils {

    /**
     * Shared Gson instance with standardized configuration. Registers case-insensitive TypeAdapters for knowledgebase enum types. This instance is thread-safe
     * and can be used concurrently.
     */
    private static final Gson GSON = createGson();

    private static Gson createGson() {
        return new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .registerTypeAdapter(SearchType.class, new CaseInsensitiveSearchTypeAdapter())
            .registerTypeAdapter(RerankingType.class, new CaseInsensitiveRerankingTypeAdapter())
            .registerTypeAdapter(DocumentStatus.class, new CaseInsensitiveDocumentStatusAdapter())
            .create();
    }

    /**
     * Case-insensitive TypeAdapter for {@link SearchType}. Matches by the enum's value field (e.g. "DENSE_VECTOR") ignoring case.
     */
    private static class CaseInsensitiveSearchTypeAdapter extends TypeAdapter<SearchType> {
        @Override
        public void write(JsonWriter out, SearchType value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.getValue());
            }
        }

        @Override
        public SearchType read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            String value = in.nextString();
            for (SearchType type : SearchType.values()) {
                if (type.getValue().equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new JsonSyntaxException("Unknown SearchType: " + value);
        }
    }

    /**
     * Case-insensitive TypeAdapter for {@link RerankingType}. Matches the enum's value field (e.g. "RRF", "MODEL", "WEIGHT") ignoring case.
     */
    private static class CaseInsensitiveRerankingTypeAdapter extends TypeAdapter<RerankingType> {
        @Override
        public void write(JsonWriter out, RerankingType value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.getValue());
            }
        }

        @Override
        public RerankingType read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            String value = in.nextString();
            for (RerankingType type : RerankingType.values()) {
                if (type.getValue().equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new JsonSyntaxException("Unknown RerankingType: " + value);
        }
    }

    /**
     * Case-insensitive TypeAdapter for {@link DocumentStatus}. Matches by the enum's value field (e.g. "PENDING", "INDEXING") ignoring case.
     */
    private static class CaseInsensitiveDocumentStatusAdapter extends TypeAdapter<DocumentStatus> {
        @Override
        public void write(JsonWriter out, DocumentStatus value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.getValue());
            }
        }

        @Override
        public DocumentStatus read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            String value = in.nextString();
            for (DocumentStatus status : DocumentStatus.values()) {
                if (status.getValue().equalsIgnoreCase(value)) {
                    return status;
                }
            }
            throw new JsonSyntaxException("Unknown DocumentStatus: " + value);
        }
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private GsonUtils() {
        // Utility class, do not instantiate
    }

    /**
     * Returns the shared Gson instance.
     *
     * @return the shared Gson instance with standardized configuration
     */
    public static Gson getGson() {
        return GSON;
    }

    /**
     * Converts the specified object to its JSON representation.
     *
     * @param obj the object to convert to JSON, can be null
     * @return the JSON representation of the object, or "null" if the object is null
     */
    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    /**
     * Parses the specified JSON string into an object of the specified class.
     *
     * @param json  the JSON string to parse, must not be null
     * @param clazz the class of the object to deserialize to, must not be null
     * @param <T>   the type of the object to deserialize to
     * @return the deserialized object, or null if the JSON is empty
     * @throws com.google.gson.JsonSyntaxException if the JSON is not valid
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

}
