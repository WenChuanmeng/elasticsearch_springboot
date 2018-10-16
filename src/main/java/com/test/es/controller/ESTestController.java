package com.test.es.controller;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping(value = "/api/es")
public class ESTestController {

    @Resource
    private TransportClient client;

    @GetMapping(value = "/getInfo")
    public Map<String, Object> getInfo(@RequestParam(value = "id")String id) {

        GetResponse response = client.prepareGet("book", "novel", id).get();

        Map<String, Object> params = new HashMap<>();

        return response.getSource();

    }

    @PutMapping(value = "/addInfo")
    public String addInfo(@RequestParam(value = "author")String author,
                          @RequestParam(value = "title")String title,
                          @RequestParam(value = "wordCount")Integer wordCount,
                          @RequestParam(value = "publishDate") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate) {

        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("author", author)
                    .field("title", title)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate.getTime())
                    .endObject();
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("book", "novel").setSource(xContentBuilder);
            IndexResponse response = indexRequestBuilder.get();
            return response.getId();
        } catch (IOException e) {
            e.printStackTrace();
            return "500";
        }
    }

    @DeleteMapping(value = "/deleteInfo")
    public String deleteInfo(@RequestParam(value = "id")String id) {

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("book", "novel", id);
        DeleteResponse deleteResponse = deleteRequestBuilder.get();
        return deleteResponse.status().toString();
    }

    @PostMapping(value = "updateInfo")
    public String updateInfo(@RequestParam(value = "id") String id,
                                          @RequestParam(value = "author")String author,
                                          @RequestParam(value = "title")String title,
                                          @RequestParam(value = "word_count")Integer wordCount,
                                          @RequestParam(value = "publish_date") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate) {

        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("author", author)

                    .endObject();

            UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("book", "novel", id).setDoc(xContentBuilder);
            UpdateResponse updateResponse = updateRequestBuilder.get();
            System.out.printf(updateResponse.status().toString());
            return updateResponse.status().toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "500";
        }
    }

    @RequestMapping(value = "queryInfos")
    public List<Map<String,Object>> queryInfos(@RequestParam(value = "author", required = false) String author,
                           @RequestParam(value = "title", required = false) String title,
                           @RequestParam(value = "gtWordCount",defaultValue = "0", required = false) Integer gtWordCount,
                           @RequestParam(value = "ltWordCount", required = false) Integer ltWordCount) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (!StringUtils.isEmpty(author)) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("author", author));
        }

        if (!StringUtils.isEmpty(title)) {
            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("title", title));
        }

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("word_count").from(gtWordCount);

        if (null != ltWordCount && ltWordCount >= 0) {
            rangeQueryBuilder.to(ltWordCount);
        }

        boolQueryBuilder.filter(rangeQueryBuilder);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("book");
        searchRequestBuilder
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .setFrom(0)
                .setSize(10);

        System.out.println(searchRequestBuilder);

        List<Map<String, Object>> results = new ArrayList<>();

        SearchResponse searchResponse = searchRequestBuilder.get();

        if (searchRequestBuilder.get() != null) {

            for (SearchHit hit : searchResponse.getHits()) {
                System.out.printf("source" + hit.getSource());
                results.add(hit.getSource());
            }
        }


        return results;
    }
}
