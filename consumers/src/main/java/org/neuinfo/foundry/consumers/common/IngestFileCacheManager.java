package org.neuinfo.foundry.consumers.common;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by bozyurt on 9/14/15.
 */
public class IngestFileCacheManager {
    DB db;
    ConcurrentNavigableMap<String, File> treeMap;
    private static IngestFileCacheManager instance;

    public synchronized static IngestFileCacheManager getInstance() {
        if (instance == null) {
            instance = new IngestFileCacheManager();
        }
        return instance;
    }

    private IngestFileCacheManager() {
        String dbFile = Parameters.getInstance().getParam("ingest.cache.db.file");
        db = DBMaker.newFileDB(new File(dbFile)).make();
        treeMap = db.getTreeMap("map");
    }

    public File get(String key) {
        File f = treeMap.get(key);
        if (f != null && !f.isFile()) {
            treeMap.remove(key);
            return null;
        }
        return f;
    }

    public void put(String key, File value) {
        treeMap.put(key, value);
        db.commit();
    }

    public void shutdown() {
        if (db != null) {
            db.close();
        }
    }

}
