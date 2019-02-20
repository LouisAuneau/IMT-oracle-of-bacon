package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Optional;

public class MongoDbRepository {
    private final MongoCollection<Document> actorCollection;

    public MongoDbRepository() {
        this.actorCollection= new MongoClient("localhost", 27017).getDatabase("oracle_of_bacon").getCollection("actors");
    }

    public Optional<Document> getActorByName(String name) {
    	FindIterable<Document> iterable = actorCollection.find(new Document("name", name));
    	Optional<Document> document;
    	if(iterable.iterator().hasNext())
    		document = Optional.of(iterable.iterator().next());
    	else
    		document = Optional.empty();
        return document;
    }
}
