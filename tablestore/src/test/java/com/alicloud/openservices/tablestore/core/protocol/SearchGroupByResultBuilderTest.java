package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.groupby.GroupByCompositeResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByCompositeResultItem;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.alicloud.openservices.tablestore.core.protocol.SearchGroupByResultBuilder.buildGroupByCompositeResult;
import static org.junit.Assert.assertEquals;

public class SearchGroupByResultBuilderTest extends BaseSearchTest {
    private final static Random random = new Random(System.currentTimeMillis());

    @Test
    public void testBuildGroupByCompositeResultItem() throws IOException {
        GroupByCompositeResultItemVerifier verifier = GroupByCompositeResultItemVerifier.newGroupByCompositeResultItemVerifier(random.nextInt(20) + 10, random.nextBoolean());

        Search.GroupByCompositeResultItem.Builder item = Search.GroupByCompositeResultItem.newBuilder()
                .addAllKeys(verifier.keys)
                .addAllIsNullKeys(verifier.isNullKeys)
                .setRowCount(verifier.rowCount);

        GroupByCompositeResultItem groupByCompositeResultItem = SearchGroupByResultBuilder.buildGroupByCompositeResultItem(item.build());
        assertEquals(verifier.rowCount, groupByCompositeResultItem.getRowCount());
        assertEquals(verifier.results.size(), groupByCompositeResultItem.getKeys().size());
        assertEquals(String.join(",", verifier.results), String.join(",", groupByCompositeResultItem.getKeys()));
    }

    @Test
    public void testBuildGroupByCompositeResult() throws IOException {
        String groupByName = "groupByComposite";
        List<GroupByCompositeResultItemVerifier> verifiers = GroupByCompositeResultItemVerifier.newGroupByCompositeResultItemVerifiers(random.nextInt(100) + 10);
        Search.GroupByCompositeResult.Builder groupByCompositeResultBuilder = Search.GroupByCompositeResult.newBuilder();
        for (GroupByCompositeResultItemVerifier verifier : verifiers) {
            groupByCompositeResultBuilder.addGroupByCompositeResultItems(Search.GroupByCompositeResultItem.newBuilder()
                    .setRowCount(verifier.rowCount)
                    .addAllIsNullKeys(verifier.isNullKeys)
                    .addAllKeys(verifier.keys));
        }

        GroupByCompositeResult result = buildGroupByCompositeResult(groupByName, groupByCompositeResultBuilder.build().toByteString());
        assertEquals(groupByName, result.getGroupByName());
        assertEquals(verifiers.size(), result.getGroupByCompositeResultItems().size());
        for (int i = 0; i < verifiers.size(); i++) {
            assertEquals(verifiers.get(i).rowCount, result.getGroupByCompositeResultItems().get(i).getRowCount());
            assertEquals(String.join(",", verifiers.get(i).results), String.join(",", result.getGroupByCompositeResultItems().get(i).getKeys()));
        }
    }

    private static class GroupByCompositeResultItemVerifier {
        private static final String[] strs = new String[]{"", "null", "NULL", "\n", "\t"};
        private long rowCount;
        private final List<String> keys = new ArrayList<>();
        private List<Boolean> isNullKeys = new ArrayList<>();
        private List<String> results = new ArrayList<>();


        public static List<GroupByCompositeResultItemVerifier> newGroupByCompositeResultItemVerifiers(int items) {
            List<GroupByCompositeResultItemVerifier> verifiers = new ArrayList<>();
            int groupFields = random.nextInt(20) + 10;
            boolean isOldVersion = random.nextBoolean();

            for (int i = 0; i < items; i++) {
                verifiers.add(newGroupByCompositeResultItemVerifier(groupFields, isOldVersion));
            }

            return verifiers;
        }

        public static GroupByCompositeResultItemVerifier newGroupByCompositeResultItemVerifier(int groupByFields, boolean isOldVersion) {
            GroupByCompositeResultItemVerifier verifier = new GroupByCompositeResultItemVerifier();
            verifier.rowCount = random.nextInt(1000);

            for (int i = 0; i < groupByFields; i++) {
                String key = strs[random.nextInt(strs.length)];
                boolean isNullKey = random.nextBoolean();
                verifier.keys.add(key);
                verifier.isNullKeys.add(isNullKey);
                if (!isNullKey) {
                    verifier.results.add(key);
                } else {
                    verifier.results.add(null);
                }
            }

            if (isOldVersion) {
                verifier.isNullKeys = Collections.emptyList();
                verifier.results = verifier.keys;
            }

            return verifier;
        }
    }
}
