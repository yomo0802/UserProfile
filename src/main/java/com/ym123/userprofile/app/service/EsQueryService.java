package com.ym123.userprofile.app.service;

import com.alibaba.fastjson.JSON;
import com.ym123.userprofile.app.support.EsQueryTag;
import com.ym123.userprofile.etls.UserTagEtl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author yomo
 * @create 2021-05-10 21:31
 */
@Service
public class EsQueryService {
    @Resource(name = "highLevelClient")
    RestHighLevelClient highLevelClient;

    public List<UserTagEtl.MemberTag> buildQuery(List<EsQueryTag> tags) {
        SearchRequest request = new SearchRequest();

        request.indices("usertag");
        request.types("_doc");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        request.source(builder);
        String[] includes = {"memberId", "phone"};
        builder.fetchSource(includes, null);
        builder.from(0);    // 查询结果，从0开始
        builder.size(1000);   // 大小为1000，相当于 limit 1000

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        builder.query(boolQueryBuilder);
        List<QueryBuilder> should = boolQueryBuilder.should();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        List<QueryBuilder> must = boolQueryBuilder.must();

        // 遍历查询条件参数，判断是哪一类
        for (EsQueryTag tag : tags) {
            String name = tag.getName();
            String value = tag.getValue();
            String type = tag.getType();

            if (type.equals("match")) {
                should.add(QueryBuilders.matchQuery(name, value));
            }
            if (type.equals("notMatch")) {
                mustNot.add(QueryBuilders.matchQuery(name, value));
            }
            if (type.equals("rangeBoth")) {
                String[] split = value.split("-");
                String v1 = split[0];
                String v2 = split[1];
                should.add(QueryBuilders.rangeQuery(name).lte(v2).gte(v1));
            }
            if (type.equals("rangeGte")) {
                should.add(QueryBuilders.rangeQuery(name).gte(value));
            }
            if (type.equals("rangeLte")) {
                should.add(QueryBuilders.rangeQuery(name).lte(value));
            }
            if (type.equals("exists")) {
                should.add(QueryBuilders.existsQuery(name));
            }
        }

        RequestOptions options = RequestOptions.DEFAULT;
        List<UserTagEtl.MemberTag> memberTags = new ArrayList<>();
        try {
            SearchResponse search = highLevelClient.search(request, options);
            SearchHits hits = search.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while (iterator.hasNext()) {
                SearchHit hit = iterator.next();
                String sourceAsString = hit.getSourceAsString();
                UserTagEtl.MemberTag memberTag = JSON.parseObject(sourceAsString, UserTagEtl.MemberTag.class);
                memberTags.add(memberTag);
            }
            return memberTags;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

