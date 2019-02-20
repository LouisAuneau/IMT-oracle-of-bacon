package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import net.codestory.http.convert.TypeConvert;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.ws.Response;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    
    private static boolean readingInitialLine = true;
    private static BulkRequest bulk;
    
    private static final String INDEX_NAME = "actors";
    private static final String TYPE_NAME = "actor";

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        // Parsing CSV file in arg
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }
        String inputFilePath = args[0];
        
        // Creating index
        resetIndexState(client);
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                    	if(!readingInitialLine) {
                    		// Sends actors every 500 actors
                    		if(count.get() % 500 == 0 && count.get() > 0) {
                    			sendAndResetBulk(client);
                    		}
                    		
                    		// Add actor to bulk
                    		IndexRequest index = new IndexRequest(INDEX_NAME, TYPE_NAME);
                    		line = line.substring(1, line.length() - 1);
                    		
                    		String jsonString = 
                    		"{" +
                    		"  \"name\": \"" + line + "\"," +
                    		"  \"suggest\": " + TypeConvert.toJson(line.split(" ")) +
                    		"}";
                    		index.source(jsonString, XContentType.JSON);
                    		bulk.add(index);
                    		
                    		// Increment counter
                    		count.incrementAndGet();
                    	} else {
                    		// Initialisation phase
                    		bulk = new BulkRequest();
                    		readingInitialLine = false;
                    	}
                    });
        }

        client.close();
    }
   
    /**
     * Delete all previous existing index of actors and creates a fresh new one. This avoids duplicates.
     * @param client Client ElasticSearch to execute requests.
     * @throws IOException
     */
    private static void resetIndexState(RestHighLevelClient client) throws IOException {
    	GetIndexRequest searchIndex = new GetIndexRequest();
    	searchIndex.indices(INDEX_NAME);
    	boolean indexAlreadyExists = client.indices().exists(searchIndex, RequestOptions.DEFAULT);
    	 
    	if(indexAlreadyExists) {
    		DeleteIndexRequest delete = new DeleteIndexRequest(INDEX_NAME);
    		client.indices().delete(delete, RequestOptions.DEFAULT);
    	}
    	
    	CreateIndexRequest createIndex = new CreateIndexRequest(INDEX_NAME);
    	client.indices().create(createIndex, RequestOptions.DEFAULT);
    	
    	// Avoid replicas sicne we are in a single node cluster, and thus avoid yellow state.
    	UpdateSettingsRequest settingsIndex = new UpdateSettingsRequest(INDEX_NAME);
    	settingsIndex.settings("{\"index.number_of_replicas\": \"0\"}", XContentType.JSON);
    	client.indices().putSettings(settingsIndex, RequestOptions.DEFAULT);
    	
    	// Put Mapping
    	PutMappingRequest mapping = new PutMappingRequest(INDEX_NAME);
    	mapping.type(TYPE_NAME);
        mapping.source(
        "{\n" +
        "  \"properties\": {\n" +
        "    \"name\": {\n" +
        "      \"type\": \"text\"\n" +
        "    },\n" +
        "    \"suggest\": {\n" +
        "      \"type\": \"completion\"\n" +
        "    }\n" +
        "  }\n" +
        "}", 
        XContentType.JSON);
        client.indices().putMapping(mapping, RequestOptions.DEFAULT);
    }
    
    private static void sendAndResetBulk(RestHighLevelClient client) {
    	try {
			BulkResponse response = client.bulk(bulk, RequestOptions.DEFAULT);// Last bulk sent.
			System.out.println("Inserted total of " + count.get() + " actors");
			bulk = new BulkRequest();
		} catch (IOException e) {
			System.err.println(e);
			e.printStackTrace();
		}
    }
}
