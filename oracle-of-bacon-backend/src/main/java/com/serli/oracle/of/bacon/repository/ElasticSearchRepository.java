package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;
    private final String SUGGEST_FIELD = "suggest";
    private final String NAME_FIELD = "name";
    private final String SUGGEST_QUERY_LOCATOR = "actors";

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder<?> suggestionBuilder = SuggestBuilders
                .completionSuggestion(SUGGEST_FIELD)
                .prefix(searchQuery, Fuzziness.AUTO);

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(SUGGEST_QUERY_LOCATOR, suggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Suggest suggest = searchResponse.getSuggest();
        CompletionSuggestion completionSuggestion = suggest.getSuggestion(SUGGEST_QUERY_LOCATOR);

        return completionSuggestion
                .getEntries()
                .stream()
                .flatMap(
                        e -> e
                            .getOptions()
                            .stream()
                            .map(o -> o
                                        .getHit()
                                        .getSourceAsMap()
                                        .get(NAME_FIELD)
                                        .toString()
                            )
                )
                .collect(Collectors.toList());
    }
}
