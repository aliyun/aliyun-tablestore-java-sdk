package com.alicloud.openservices.tablestore.model.memory;

public class SearchHit {
    private MemoryUnit unit;
    private Double score;
    private Double similarity;
    private Double rankingScore;
    private RankingSignals rankingSignals;
    private String source;

    public SearchHit() {
    }

    public MemoryUnit getUnit() {
        return unit;
    }

    public void setUnit(MemoryUnit unit) {
        this.unit = unit;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(Double similarity) {
        this.similarity = similarity;
    }

    public Double getRankingScore() {
        return rankingScore;
    }

    public void setRankingScore(Double rankingScore) {
        this.rankingScore = rankingScore;
    }

    public RankingSignals getRankingSignals() {
        return rankingSignals;
    }

    public void setRankingSignals(RankingSignals rankingSignals) {
        this.rankingSignals = rankingSignals;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
