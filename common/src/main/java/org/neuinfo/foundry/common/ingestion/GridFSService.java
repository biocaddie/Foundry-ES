package org.neuinfo.foundry.common.ingestion;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Scanner;

/**
 * Created by bozyurt on 5/14/15.
 */
public class GridFSService extends BaseIngestionService {
    static Logger log = Logger.getLogger(GridFSService.class);


    public ObjectId saveJsonFile(JSONObject json, String filename) throws Exception {
        String jsonStr = json.toString();
        //String md5 = Utils.getMD5ChecksumOfString(jsonStr);
        DB db = mongoClient.getDB(dbName);
        GridFS gridFS = new GridFS(db);
        InputStream in = null;
        try {
            in = new ByteArrayInputStream(jsonStr.getBytes(Charset.forName("UTF-8")));
            GridFSInputFile gridFSFile = gridFS.createFile(in, filename);
            //BasicDBObject metadata = new BasicDBObject("md5", md5);
            //gridFSFile.setMetaData(metadata);
            gridFSFile.save();
            ObjectId id = (ObjectId) gridFSFile.getId();
            System.out.println("id:" + id);
            return id;
        } catch (MongoException me) {
            log.error("saveJsonFile:" + filename, me);
            throw new Exception(me.getMessage());
        } finally {
            Utils.close(in);
        }
    }


    public boolean isSame(JSONObject json, ObjectId objectId) {
        DB db = mongoClient.getDB(dbName);
        GridFS gridFS = new GridFS(db);

        GridFSDBFile gridFSDBFile = gridFS.find(objectId);
        String jsonStr = json.toString();
        String md5 = null;
        try {
            md5 = Utils.getMD5ChecksumOfString(jsonStr);
        } catch (Exception e) {
            return false;
        }
        return gridFSDBFile.getMD5().equals(md5);
    }


    public JSONObject findJSONFile(ObjectId objectId) {
        DB db = mongoClient.getDB(dbName);
        GridFS gridFS = new GridFS(db);

        GridFSDBFile gridFSDBFile = gridFS.find(objectId);
        if (gridFSDBFile == null) {
            return null;
        }
        InputStream in = null;
        try {
            in = gridFSDBFile.getInputStream();
            Scanner scanner = new Scanner(in).useDelimiter("\\A");
            if (scanner.hasNext()) {
                return new JSONObject(scanner.next());
            }
        } catch (Throwable t) {
            log.error("findJSONFile id:" + objectId.toString(), t);
        } finally {
            Utils.close(in);
        }
        return null;
    }


    public boolean deleteJSONFile(ObjectId objectId) {
        DB db = mongoClient.getDB(dbName);
        GridFS gridFS = new GridFS(db);
        try {
            gridFS.remove(objectId);
        } catch (MongoException x) {
            log.error("deleteJSONFile id:" + objectId.toString(), x);
            return false;
        }
        return true;
    }

    public void deleteDuplicates(ObjectId objectId, String filename) {
        DB db = mongoClient.getDB(dbName);
        GridFS gridFS = new GridFS(db);
        List<GridFSDBFile> gridFSDBFiles = gridFS.find(new BasicDBObject("filename", filename));
        if (gridFSDBFiles.size() > 1) {
            System.out.println("gridFSDBFiles.size" + gridFSDBFiles.size());
            for(GridFSDBFile gfile : gridFSDBFiles) {
                if (!gfile.getId().toString().equals(objectId.toString())) {
                    System.out.println("duplicate file:" + gfile.toString());
                }
            }
        }
    }

    public void setMongoClient(MongoClient mc) {
        Assertion.assertTrue(this.mongoClient == null);
        this.mongoClient = mc;
    }

    public void setMongoDBName(String dbName) {
        this.dbName = dbName;
    }
}
