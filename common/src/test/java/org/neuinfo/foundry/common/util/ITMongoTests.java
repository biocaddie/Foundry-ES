package org.neuinfo.foundry.common.util;

import com.mongodb.*;
import junit.framework.TestCase;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.ingestion.Configuration;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 5/28/14.
 */
public class ITMongoTests extends TestCase {
    private MongoClient mongoClient;
    private String mongoDbName;

    public ITMongoTests(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        Configuration conf = Configuration.fromXML("common-cfg.xml");
        this.mongoDbName = conf.getMongoDBName();
        List<ServerAddress> servers = new ArrayList<ServerAddress>(conf.getServers().size());
        for (ServerInfo si : conf.getServers()) {
            InetAddress inetAddress = InetAddress.getByName(si.getHost());
            servers.add(new ServerAddress(inetAddress, si.getPort()));
        }
        mongoClient = new MongoClient(servers);

        mongoClient.setWriteConcern(WriteConcern.SAFE);

        DB db = mongoClient.getDB(mongoDbName);
        DBCollection products = db.getCollection("products");
        for (int i = 0; i < 10; i++) {
            String prodName = "product_" + i;
            if (!hasProduct(prodName, products)) {
                BasicDBObject bdo = new BasicDBObject();
                bdo.put("name", prodName);
                bdo.put("price", (int) (100 * Math.random()) + 1);
                bdo.put("numSold", 0);
                BasicDBList sellers = new BasicDBList();
                sellers.add(new BasicDBObject("name", "Best Buy"));
                sellers.add(new BasicDBObject("name", "Staples"));
                bdo.put("sellers", sellers);
                products.insert(bdo);
            }
        }
    }

    boolean hasProduct(String prodName, DBCollection products) {
        BasicDBObject query = new BasicDBObject("name", prodName);
        final DBCursor cursor = products.find(query);
        boolean found = false;
        try {
            if (cursor.hasNext()) {
                found = true;
            }
        } finally {
            cursor.close();
        }
        return found;
    }

    public void tearDown() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        super.tearDown();
    }

    public void testDBObjectSelector() {
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("primaryKey", "zetterbergjansenritmodel");
        final DBCursor cursor = records.find(query);

        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                final Object batchId = JSONUtils.findNested(dbObject, "History.batchId");
                final Object sourceId = JSONUtils.findNested(dbObject, "SourceInfo.SourceID");
                System.out.println("batchId:" + batchId + " sourceId:" + sourceId);

                assertNotNull(batchId);
                assertNotNull(sourceId);
            }
        } finally {
            cursor.close();
        }
    }


    public void testIncr() {
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection products = db.getCollection("products");
        BasicDBObject query = new BasicDBObject("name", "product_1");
        BasicDBObject incValue = new BasicDBObject("numSold", 1);
        BasicDBObject numSoldUpdate = new BasicDBObject("$inc", incValue);

        products.update(query, numSoldUpdate);
        final DBCursor cursor = products.find(query);

        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                System.out.println(dbObject.toString());
            } else {
                fail("No record for product_1");
            }

        } finally {
            cursor.close();
        }
    }

    public void testProjection() {
        String prodName = "product_1";
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection products = db.getCollection("products");
        BasicDBObject query = new BasicDBObject("name", prodName);
        BasicDBObject keys = new BasicDBObject("sellers", 1);

        final DBCursor cursor = products.find(query, keys);
        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                System.out.println(dbObject.toString());
            } else {
                fail("No record for " + prodName);
            }

        } finally {
            cursor.close();
        }
    }

    public void testPush2Array() {
        String prodName = "product_2";
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection products = db.getCollection("products");
        BasicDBObject query = new BasicDBObject("name", prodName);
        BasicDBObject addSeller = new BasicDBObject("sellers", new BasicDBObject("name", "Sears"));
        products.update(query, new BasicDBObject("$push", addSeller));

        final DBCursor cursor = products.find(query);
        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                System.out.println(dbObject.toString());
            } else {
                fail("No record for " + prodName);
            }
        } finally {
            cursor.close();
        }
    }

    public void testSubdocUpdate() {
        String prodName = "product_1";
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection products = db.getCollection("products");
        BasicDBObject query = new BasicDBObject("name", prodName);
        BasicDBList sellers = new BasicDBList();
        sellers.add(new BasicDBObject("name", "Sears"));
        BasicDBObject sellersObj = new BasicDBObject("sellers", sellers);

        products.update(query, new BasicDBObject("$set", sellersObj));

        final DBCursor cursor = products.find(query);
        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                System.out.println(dbObject.toString());
            } else {
                fail("No record for " + prodName);
            }
        } finally {
            cursor.close();
        }
    }
}
