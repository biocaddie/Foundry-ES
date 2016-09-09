package org.neuinfo.foundry.consumers;

import com.mongodb.*;

import java.util.Set;

/**
 * Created by bozyurt on 4/4/14.
 */
public class EventStreamerTest {

    public static void testQuery() throws Exception {
        MongoClient mc = null;
        try {
            mc = new MongoClient("localhost", 27017);
            DB db = mc.getDB("users");
            Set<String> collections = db.getCollectionNames();
            for(String colName : collections) {
                System.out.println(colName);
            }
            DBCollection users = db.getCollection("users");
            DBCursor cursor = users.find();
            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next());
                }

            } finally {
                cursor.close();
            }



        } finally {
             if (mc != null) {
                 mc.close();
             }
        }
    }

    public static void main(String[] args) throws Exception{
        testQuery();
    }
}
