package org.neuinfo.foundry.common.ingestion;

import com.mongodb.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.User;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by bozyurt on 5/18/16.
 */
public class UserIngestionService extends BaseIngestionService {
    public void saveUser(String username, String pwd) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection users = db.getCollection("users");
        User user = new User.Builder(username, pwd, "").role("curator").build();
        JSONObject json = user.toJSON();
        DBObject dbo = JSONUtils.encode(json, true);
        users.save(dbo);
        try {
            createApiKey(username, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void deleteUser(String username) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection users = db.getCollection("users");
        BasicDBObject query = new BasicDBObject("username", username);
        DBObject existingUser = users.findOne(query);
        if (existingUser != null) {
            users.remove(existingUser, WriteConcern.ACKNOWLEDGED);
            deleteApiKey(username);
            System.out.println("deleted " + username);
        }
    }

    public List<User> listUsers() {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection users = db.getCollection("users");
        final DBCursor cursor = users.find();
        List<User> userList = new LinkedList<User>();
        try {
            while (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                userList.add(User.fromDBObject(dbObject));
            }
        } finally {
            cursor.close();
        }
        return userList;
    }


    void deleteApiKey(String username) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection aks = db.getCollection("apiKeys");
        BasicDBObject query = new BasicDBObject("username", username);
        DBObject aksOne = aks.findOne(query);
        if (aksOne != null) {
            aks.remove(aksOne, WriteConcern.ACKNOWLEDGED);
            System.out.println("deleted akiKey for " + username);
        }
    }

    public void createApiKey(String username, boolean perpetual) throws Exception {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection aks = db.getCollection("apiKeys");
        JSONObject json = new JSONObject();
        json.put("username", username);
        json.put("perpetual", perpetual);
        String key = null;
        try {
            Random random = SecureRandom.getInstance("SHA1PRNG");
            key = Utils.getMD5ChecksumOfString(String.valueOf(random.nextLong()));
        } catch (Exception x) {
            Random random = new Random();
            key = Utils.getMD5ChecksumOfString(String.valueOf(random.nextLong()));
        }
        json.put("apiKey", key);
        DBObject dbo = JSONUtils.encode(json, true);
        aks.save(dbo);
    }
}
