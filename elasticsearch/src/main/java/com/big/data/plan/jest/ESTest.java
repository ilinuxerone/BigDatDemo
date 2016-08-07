package com.big.data.plan.jest;

import java.io.IOException;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import io.searchbox.client.JestResult;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

/**
 * test
 */
public class ESTest {

    private static JestHttpClient client = ESFactory.getClient();

    /**
     * indexing
     *
     * @param indexName
     */
    public void index(String indexName) {
        del(indexName);
        try {
            // create
            CreateIndex cIndex = new CreateIndex(new CreateIndex.Builder(indexName));
            client.execute(cIndex);
            // add doc
            for (int i = 0; i < 100; i++) {
                User user = new User();
                user.setId(new Long(i));
                user.setName("huang fox " + i);
                user.setAge(i % 100);
                Index index = new Index.Builder(user).index(indexName).type(indexName).build();
                client.execute(index);
                System.out.println("======================OK:" + i);
            }
            //client.shutdownClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void del(String indexName) {
        // drop
        try {
            DeleteIndex dIndex = new DeleteIndex(new DeleteIndex.Builder(indexName));
            client.execute(dIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * getting by id
     *
     * @param indexName
     * @param query
     */
    public void get(String indexName, String query) {
        Get get = new Get.Builder(indexName, query).build();
        try {
            JestResult rs = client.execute(get);
            System.out.println(rs.getJsonString());
            //client.shutdownClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void indexBulk(String indexName) {
        try {
            // drop
            del(indexName);
            // create
            CreateIndex cIndex = new CreateIndex(new CreateIndex.Builder(indexName));
            client.execute(cIndex);
            // add doc
            Bulk.Builder bulkBuilder = new Bulk.Builder();
            for (int i = 0; i < 1000; i++) {
                User user = new User();
                user.setId(new Long(i));
                user.setName("huang fox " + i);
                user.setAge(i % 100);
                Index index = new Index.Builder(user).index(indexName).type(indexName).build();
                bulkBuilder.addAction(index);
            }
            client.execute(bulkBuilder.build());
            //client.shutdownClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * query
     *
     * @param query
     */
    public void search(String query) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(query));
//        searchSourceBuilder.field("name");
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        try {
            JestResult rs = client.execute(search);
            System.out.println(rs.getJsonString());
            //client.shutdownClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ESTest t = new ESTest();
//        String indexName = "user";
        // indexing
//        t.index(indexName);
        // getting by id
        t.get("mytest", "0");
        System.out.println("==========================================");
        t.get("mytest", "1");
        // query
//        String query = "mytest";
//        t.search(query);
        client.shutdownClient();
    }

}
