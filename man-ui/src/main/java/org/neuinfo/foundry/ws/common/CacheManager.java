package org.neuinfo.foundry.ws.common;

import org.json.JSONObject;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by bozyurt on 8/28/15.
 */
public class CacheManager {
    DB db;
    DB db2;
    ConcurrentNavigableMap<String, JSONObject> treeMap;
    ConcurrentNavigableMap<String, Serializable> treeMap2;
    private static CacheManager instance;

    public synchronized static CacheManager getInstance() throws IOException {
        if (instance == null) {
            instance = new CacheManager();
        }
        return instance;
    }

    private CacheManager() throws IOException {
        Properties props = Utils.loadProperties("man-ui.properties");
        String cacheFile =  props.getProperty("samples.cache.file");
        String cacheFile2 =  props.getProperty("misc.cache.file");
        Assertion.assertNotNull(cacheFile);
        Assertion.assertNotNull(cacheFile2);
        db = DBMaker.newFileDB(new File(cacheFile)).make();
        treeMap = db.getTreeMap("map");
        db2 = DBMaker.newFileDB(new File(cacheFile2)).make();
        treeMap2 = db2.getTreeMap("map");
    }

    public void put(String key, Serializable value) {
        treeMap2.put(key, value);
        db2.commit();
    }

    public Serializable get(String key) {
        return treeMap2.get(key);
    }

    public JSONObject getJSON(String key) {
        return treeMap.get(key);
    }

    public void putJSON(String key, JSONObject value) {
        treeMap.put(key,value);
        db.commit();
    }

    public void shutdown() {
        if (db != null) {
            db.close();
        }
        if (db2 != null) {
            db2.close();
        }
    }
}
