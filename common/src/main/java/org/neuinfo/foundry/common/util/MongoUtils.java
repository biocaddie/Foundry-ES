package org.neuinfo.foundry.common.util;

import com.mongodb.*;
import org.neuinfo.foundry.common.model.Source;

import java.util.List;

/**
 * Created by bozyurt on 11/4/15.
 */
public class MongoUtils {
    public static Source getSource(BasicDBObject query, DBCollection sources) {
        final DBCursor cursor = sources.find(query);

        Source source = null;
        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                source = Source.fromDBObject(dbObject);
            }
        } finally {
            cursor.close();
        }
        return source;
    }


    public static MongoClient createMongoClient(List<ServerAddress> servers) {
        MongoClientOptions mco = new MongoClientOptions.Builder().socketKeepAlive(false).
                maxConnectionIdleTime(60000).connectionsPerHost(10).build();
        MongoClient mongoClient = new MongoClient(servers, mco);

        mongoClient.setWriteConcern(WriteConcern.SAFE);
        return mongoClient;
    }
}
