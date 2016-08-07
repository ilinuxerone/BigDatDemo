package com.big.data.plan.original;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Created by seven on 07/03/16.
 */
public class Search {

    public static void main(String[] args) throws Exception {
        Client client = TransportClient.builder().build().addTransportAddress(
            new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        // on shutdown
        SearchResponse response = client.prepareSearch("mytest").setTypes("test")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//          .setQuery(QueryBuilders.termQuery("multi", "test")) // Query
//          .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18)) // Filter
            .setFrom(0).setSize(100).setExplain(true).execute().actionGet();

        SearchHits aa = response.getHits();
        for (SearchHit tmp : aa.getHits()) {
            System.out.println("ID:" + tmp.getId());
            System.out.println("Score:" + tmp.score());
            System.out.println("source:" + tmp.getSourceAsString());
            System.out.println("=======================================");
        }
        client.close();
    }
}
