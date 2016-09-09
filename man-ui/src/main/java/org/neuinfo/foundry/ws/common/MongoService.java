package org.neuinfo.foundry.ws.common;

import com.mongodb.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.ConfigLoader;
import org.neuinfo.foundry.common.config.Configuration;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.model.*;
import org.neuinfo.foundry.common.util.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 8/25/15.
 */
public class MongoService {
    MongoClient mongoClient;
    String dbName;
    private String queueName;
    private Configuration config;
    private static MongoService instance = null;
    private final static Logger logger = Logger.getLogger(MongoService.class);


    public synchronized static MongoService getInstance() throws Exception {
        if (instance == null) {
            instance = new MongoService("foundry.consumer.head");
        }
        return instance;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public String getDbName() {
        return dbName;
    }

    private MongoService(String queueName) throws Exception {
        this.queueName = queueName;
        this.config = ConfigLoader.load("man-ui-cfg.xml");
        this.dbName = config.getMongoDBName();
        List<ServerAddress> servers = new ArrayList<ServerAddress>(config.getServers().size());
        for (ServerInfo si : config.getServers()) {
            InetAddress inetAddress = InetAddress.getByName(si.getHost());
            servers.add(new ServerAddress(inetAddress, si.getPort()));
        }
        mongoClient = MongoUtils.createMongoClient(servers);
    }

    public synchronized void shutdown() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }


    public List<JSONObject> getAllIngestorConfigs() {
        List<JSONObject> list = new LinkedList<JSONObject>();
        DB db = mongoClient.getDB(dbName);
        DBCollection ics = db.getCollection("ingestorConfigs");
        DBCursor cursor = ics.find(new BasicDBObject());
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                JSONObject js = JSONUtils.toJSON(dbo, false);
                IngestConfig ic = IngestConfig.fromJSON(js);
                list.add(ic.toJSON());
            }
        } finally {
            cursor.close();
        }
        return list;
    }


    public List<ApiKeyInfo> getAllApikeyInfos() {
        DB db = mongoClient.getDB(dbName);
        DBCollection aks = db.getCollection("apiKeys");
        List<ApiKeyInfo> akiList = new LinkedList<ApiKeyInfo>();
        DBCursor cursor = aks.find(new BasicDBObject());
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                JSONObject js = JSONUtils.toJSON(dbo, false);
                ApiKeyInfo aki = ApiKeyInfo.fromJSON(js);
                akiList.add(aki);
            }
        } finally {
            cursor.close();
        }
        return akiList;
    }

    public ApiKeyInfo findApiKeyInfoForUser(String username) {
        BasicDBObject query = new BasicDBObject("username", username);
        return getApiKeyInfo(query);
    }

    public ApiKeyInfo findApiKeyInfoByApiKey(String apiKey) {
        BasicDBObject query = new BasicDBObject("apiKey", apiKey);
        return getApiKeyInfo(query);
    }

    ApiKeyInfo getApiKeyInfo(BasicDBObject query) {
        DB db = mongoClient.getDB(dbName);
        DBCollection aks = db.getCollection("apiKeys");
        DBObject akDBO = aks.findOne(query);
        if (akDBO != null) {
            return ApiKeyInfo.fromDBObject(akDBO);
        }
        return null;
    }


    public User findUser(String userName, String pwd) {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        BasicDBObject query = new BasicDBObject("username", userName);
        if (pwd != null) {
            query.append("password", pwd);
        }
        DBObject userDBO = users.findOne(query);
        if (userDBO != null) {
            User user = User.fromDBObject(userDBO);
            return user;
        }
        return null;
    }

    public List<User> getAllUsers() {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        DBCursor cursor = users.find(new BasicDBObject());
        List<User> userList = new ArrayList<User>();
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                userList.add(User.fromDBObject(dbo));
            }
        } finally {
            cursor.close();
        }
        return userList;
    }

    public void saveUser(User user) {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        BasicDBObject query = new BasicDBObject("username", user.getUsername());
        DBObject existingUserDBO = users.findOne(query);
        if (existingUserDBO == null) {
            JSONObject json = user.toJSON();
            DBObject userDBO = JSONUtils.encode(json, true);
            users.insert(userDBO);
        } else {
            // update
            if (!Utils.isEmpty(user.getPassword())) {
                existingUserDBO.put("password", user.getPassword());
            }
            if (!Utils.isEmpty(user.getEmail())) {
                existingUserDBO.put("email", user.getEmail());
            }
            if (!Utils.isEmpty(user.getRole())) {
                existingUserDBO.put("role", user.getRole());
            }
            users.update(query, existingUserDBO);
        }
    }

    public boolean saveSource(Source source) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", source.getResourceID());
        DBObject srcDBO = sources.findOne(query);
        if (srcDBO == null) {
            JSONObject json = source.toJSON();
            DBObject sourceDbObj = JSONUtils.encode(json, true);
            sources.insert(sourceDbObj);
            return true;
        }
        return false;
    }

    public boolean updateSource(String sourceId, String dataSource, JSONObject payload) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", sourceId);
        if (dataSource != null) {
            query.append("sourceInformation.dataSource", dataSource);
        }
        DBObject srcDBO = sources.findOne(query);
        if (srcDBO != null) {
            JSONObject paramsJSON = payload.getJSONObject("params");
            String sourceName = payload.getString("sourceName");
            BasicDBObject siDBO = (BasicDBObject) srcDBO.get("sourceInformation");
            siDBO.put("name", sourceName);

            BasicDBObject csDBO = (BasicDBObject) srcDBO.get("contentSpecification");
            csDBO.clear();
            for (String key : paramsJSON.keySet()) {
                csDBO.put(key, paramsJSON.getString(key));
            }
            sources.update(query, srcDBO);
            return true;
        }
        return false;
    }


    public boolean updateSource(String sourceId, String dataSource, List<String> pkJSONPaths, String transformScript) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", sourceId);
        if (dataSource != null) {
            query.append("sourceInformation.dataSource", dataSource);
        }
        DBObject srcDBO = sources.findOne(query);
        if (srcDBO != null) {
            if (!Utils.isEmpty(transformScript)) {
                BasicDBObject tiDBO = (BasicDBObject) srcDBO.get("transformationInfo");
                tiDBO.put("transformScript", transformScript);
            }
            if (!pkJSONPaths.isEmpty()) {
                BasicDBObject oriDBO = (BasicDBObject) srcDBO.get("originalRecordIdentifierSpec");
                BasicDBList fields = (BasicDBList) oriDBO.get("fields");
                fields.clear();
                for (String pkJSONPath : pkJSONPaths) {
                    fields.add(pkJSONPath);
                }
            }
            sources.update(query, srcDBO);
            return true;
        }
        return false;
    }

    public Source findSource(String sourceId, String dataSource) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", sourceId);
        if (dataSource != null) {
            query.append("sourceInformation.dataSource", dataSource);
        }
        DBObject srcDBO = sources.findOne(query);
        if (srcDBO != null) {
            return Source.fromDBObject(srcDBO);
        }
        return null;
    }

    public List<JSONObject> getSourceSummaries() {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject keys = new BasicDBObject("sourceInformation", 1)
                .append("transformationInfo", 1)
                .append("originalRecordIdentifierSpec", 1);
        DBCursor cursor = sources.find(new BasicDBObject(), keys);
        List<JSONObject> summaries = new LinkedList<JSONObject>();
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                BasicDBObject siDBO = (BasicDBObject) dbo.get("sourceInformation");
                BasicDBObject tiDBO = (BasicDBObject) dbo.get("transformationInfo");
                BasicDBObject pkDBO = (BasicDBObject) dbo.get("originalRecordIdentifierSpec");
                JSONObject json = new JSONObject();
                json.put("sourceID", siDBO.getString("resourceID"));
                json.put("name", siDBO.getString("name"));
                json.put("dataSource", siDBO.getString("dataSource"));
                String repositoryID = siDBO.getString("repositoryID");
                json.put("repositoryID", repositoryID != null ? repositoryID : "");
                if (tiDBO != null) {
                    json.put("transformScript", tiDBO.getString("transformScript"));
                }
                if (pkDBO != null) {
                    JSONObject js = JSONUtils.toJSON(pkDBO, false);
                    JSONArray fields = js.getJSONArray("fields");
                    json.put("primaryKeyJSONPath", fields);
                }
                summaries.add(json);
            }
        } finally {
            cursor.close();
        }
        return summaries;
    }

    public List<JSONObject> getSourceSummaries4Dashboard() {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject keys = new BasicDBObject("sourceInformation", 1)
                .append("batchInfos", 1);
        DBCursor cursor = sources.find(new BasicDBObject(), keys);
        List<JSONObject> summaries = new LinkedList<JSONObject>();
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                BasicDBObject siDBO = (BasicDBObject) dbo.get("sourceInformation");
                BasicDBList biList = (BasicDBList) dbo.get("batchInfos");
                JSONObject json = new JSONObject();
                json.put("sourceID", siDBO.getString("resourceID"));
                json.put("name", siDBO.getString("name"));
                json.put("dataSource", siDBO.getString("dataSource"));
                String repositoryID = siDBO.getString("repositoryID");
                json.put("repositoryID", repositoryID != null ? repositoryID : "");
                JSONArray biJSONArr = new JSONArray();
                for(int i = 0; i < biList.size(); i++) {
                    DBObject biDBO = (DBObject) biList.get(i);
                    BatchInfo bi = BatchInfo.fromDbObject(biDBO);
                    biJSONArr.put( bi.toJSON());
                }
                json.put("batchInfos", biJSONArr);
                summaries.add(json);
            }
        } finally {
            cursor.close();
        }
        return summaries;
    }

    public void ingestSource(String sourceID) throws Exception {
        Source source = findSource(sourceID, null);
        Assertion.assertNotNull(source);
        JSONObject json = MessagingUtils.prepareMessageBody("ingest", source, this.config.getWorkflowMappings());
        sendMessage(json);
    }


    public void sendMessage(JSONObject messageBody) throws JMSException {
        sendMessage(messageBody, this.queueName);
    }


    private void sendMessage(JSONObject messageBody, String queue2Send) throws JMSException {
        Connection con = null;
        Session session = null;
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(config.getBrokerURL());
            con = factory.createConnection();

            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);
            Destination destination = session.createQueue(queue2Send);
            if (logger.isInfoEnabled()) {
                logger.info("sending user JMS message with payload:" + messageBody.toString(2) +
                        " to queue:" + this.queueName);
            }
            Message message = session.createObjectMessage(messageBody.toString());
            producer.send(destination, message);
        } finally {
            if (session != null) {
                session.close();
            }
            if (con != null) {
                con.close();
            }
        }
    }


}
