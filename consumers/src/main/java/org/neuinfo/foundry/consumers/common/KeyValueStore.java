package org.neuinfo.foundry.consumers.common;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by bozyurt on 5/10/16.
 */
public class KeyValueStore<T> {
    private DB db;

    public KeyValueStore(String dbFile) {
        db = DBMaker.newFileDB(new File(dbFile)).make();
    }

    public T get(String tableName, String key) {
        ConcurrentNavigableMap<String, T> treeMap = db.getTreeMap(tableName);
        return treeMap.get(key);
    }

    public void put(String tableName, String key, T value) {
        ConcurrentNavigableMap<String, T> treeMap = db.getTreeMap(tableName);
        treeMap.put(key, value);
        db.commit();
    }


    public void shutdown() {
        if (db != null) {
            db.close();
        }
    }
}
