package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 7/1/15.
 */
public class UtilsTest extends TestCase {
    public UtilsTest(String name) {
        super(name);
    }

    public void testTransformedExtraction() throws Exception {
        File outDir = new File("/tmp/biocaddie_pdb");
        outDir.mkdir();
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            DB db = helper.getDB();
            DBCollection collection = db.getCollection("nifRecords");
            BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", "nif-0000-00135");

            BasicDBObject keys = new BasicDBObject("Data", 1);
            keys.put("primaryKey",1);
            DBCursor dbCursor = collection.find(query, keys);
            try {
                while (dbCursor.hasNext()) {
                    BasicDBObject dbo = (BasicDBObject) dbCursor.next();
                    BasicDBObject data = (BasicDBObject) dbo.get("Data");
                    BasicDBObject transformedRec = (BasicDBObject) data.get("transformedRec");
                    String primaryKey = (String) dbo.get("primaryKey");
                    if (transformedRec == null) {
                        System.err.println("no transformedRec for " + primaryKey);
                        continue;
                    }
                    JSONObject json = JSONUtils.toJSON(transformedRec, false);
                    // System.out.println(json.toString(2));
                    File f = new File(outDir, primaryKey + ".json");
                    Utils.saveText(json.toString(2), f.getAbsolutePath());
                    System.out.println("saved to " + f.getAbsolutePath());
                    // break;
                }
            } finally {
                dbCursor.close();
            }

        } finally {
            helper.shutdown();
        }

    }
}
