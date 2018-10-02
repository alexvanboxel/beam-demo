package org.apache.beam.sdk.io.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.bson.Document;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.PBegin;
import org.bson.Document;
import org.joda.time.Instant;

import java.util.List;

/**
 *
 */
@DoFn.UnboundedPerElement
public class MongoDbChangeStreamFn extends DoFn<String, String> {

    protected String MONGO_URL;

    public MongoDbChangeStreamFn(String MONGO_URL) {
        this.MONGO_URL = MONGO_URL;
    }

    private transient MongoClient mongoClient;

    private transient MongoCursor<ChangeStreamDocument> cursor;

    @Setup
    public void setup() {
        // ...
        System.out.println();


        MongoClientURI connectionString = new MongoClientURI(MONGO_URL);

        mongoClient = new MongoClient(connectionString);
        MongoDatabase db = mongoClient.getDatabase("staging-carts");


        MongoCollection collection = db.getCollection("carts");

        ChangeStreamIterable watch = collection.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        cursor = watch.iterator();

    }

    @Teardown
    public void teardown() {
        cursor.close();
        mongoClient.close();
    }

    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {

        //  var projectId = "quantum-cloud-test"


//        MongoDatabase db = mongoClient.getDatabase("staging-carts");
//
//
//        MongoCollection collection = db.getCollection("carts");
//
//        ChangeStreamIterable watch = collection.watch();
//
//
//        MongoCursor<ChangeStreamDocument> cursor = collection.watch().iterator();
//
//
//        cursor.forEachRemaining((ChangeStreamDocument x) -> {
//            String json = ((Document) x.getFullDocument()).toJson();
//            System.out.println(json);
//            c.output(json);
//        });

//        for (MongoCus changeStreamDocument :  ) {
//            //println(changeStreamDocument)
//        }

//
//
//

        System.out.println(tracker.currentRestriction().toString());
        for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {

            ChangeStreamDocument next = cursor.tryNext();
            if (next != null) {
                c.updateWatermark(Instant.now());
                Object fullDoc = next.getFullDocument();
                if (fullDoc != null) {
                    String json = ((Document) fullDoc).toJson();
                    System.out.println("OpType" + next.getOperationType().toString());
                    c.outputWithTimestamp(json, Instant.now());
                } else {
                    System.err.println("No full document");
                }
            } else {
                System.err.println("No element for " + i + " at " + System.currentTimeMillis());
            }
        }
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(System.currentTimeMillis(), Long.MAX_VALUE);
    }
}
