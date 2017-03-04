package org.neuinfo.foundry.jms.common;


import com.sun.org.apache.regexp.internal.RE;
import org.json.JSONObject;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neuinfo.foundry.common.model.SourceProcessStatusInfo;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by bozyurt on 2/1/17.
 */
public class RecoveryManager {
    DB db;
    ConcurrentNavigableMap<String, String> treeMap;
    private static RecoveryManager instance = null;

    public synchronized static RecoveryManager getInstance() {
        if (instance == null) {
            instance = new RecoveryManager();
        }
        return instance;
    }

    public RecoveryManager() {
        Properties props = null;
        try {
            props = Utils.loadProperties("dispatcher.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String dbFile = props.getProperty("recovery.cache.db.file");
        db = DBMaker.newFileDB(new File(dbFile)).make();
        treeMap = db.getTreeMap("map");
    }

    public SourceProcessStatusInfo get(String key) {
        String jsonStr = treeMap.get(key);
        if (jsonStr != null) {
            return SourceProcessStatusInfo.fromJSON(new JSONObject(jsonStr));
        }
        return null;
    }

    public void put(String key, SourceProcessStatusInfo spsi) {
        String jsonStr = spsi.toJSON().toString();
        treeMap.put(key, jsonStr);
        db.commit();
    }

    public void shutdown() {
        if (db != null) {
            db.close();
        }
    }

}
