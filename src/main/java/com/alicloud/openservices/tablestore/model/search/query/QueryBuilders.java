package com.alicloud.openservices.tablestore.model.search.query;

public final class QueryBuilders {

    public static BoolQuery.Builder bool() {
        return BoolQuery.newBuilder();
    }

    public static ConstScoreQuery.Builder constScore() {
        return ConstScoreQuery.newBuilder();
    }

    public static ExistsQuery.Builder exists(String fieldName) {
        return ExistsQuery.newBuilder().field(fieldName);
    }

    public static FunctionScoreQuery.Builder functionScore(String fieldName) {
        return FunctionScoreQuery.newBuilder().fieldValueFactor(fieldName);
    }

    public static FunctionsScoreQuery.Builder functionsScore() {
        return FunctionsScoreQuery.newBuilder();
    }

    public static GeoBoundingBoxQuery.Builder geoBoundingBox(String fieldName) {
        return GeoBoundingBoxQuery.newBuilder().field(fieldName);
    }

    public static GeoDistanceQuery.Builder geoDistance(String fieldName) {
        return GeoDistanceQuery.newBuilder().field(fieldName);
    }

    public static GeoPolygonQuery.Builder geoPolygon(String fieldName) {
        return GeoPolygonQuery.newBuilder().field(fieldName);
    }

    public static MatchAllQuery.Builder matchAll() {
        return MatchAllQuery.newBuilder();
    }

    public static MatchPhraseQuery.Builder matchPhrase(String fieldName, String text) {
        return MatchPhraseQuery.newBuilder().field(fieldName).text(text);
    }

    public static MatchQuery.Builder match(String fieldName, String text) {
        return MatchQuery.newBuilder().field(fieldName).text(text);
    }

    public static NestedQuery.Builder nested() {
        return NestedQuery.newBuilder();
    }

    public static PrefixQuery.Builder prefix(String fieldName, String prefix) {
        return PrefixQuery.newBuilder().field(fieldName).prefix(prefix);
    }

    public static SuffixQuery.Builder suffix(String fieldName, String suffix) {
        return SuffixQuery.newBuilder().field(fieldName).suffix(suffix);
    }

    public static RangeQuery.Builder range(String fieldName) {
        return RangeQuery.newBuilder().field(fieldName);
    }

    public static TermQuery.Builder term(String fieldName, Object termValue) {
        return TermQuery.newBuilder().field(fieldName).term(termValue);
    }

    public static TermsQuery.Builder terms(String fieldName) {
        return TermsQuery.newBuilder().field(fieldName);
    }

    public static WildcardQuery.Builder wildcard(String fieldName, String value) {
        return WildcardQuery.newBuilder().field(fieldName).value(value);
    }

    public static KnnVectorQuery.Builder knnVector(String fieldName, int topK, float[] queryVector) {
        return KnnVectorQuery.newBuilder().field(fieldName).topK(topK).queryVector(queryVector);
    }
}
