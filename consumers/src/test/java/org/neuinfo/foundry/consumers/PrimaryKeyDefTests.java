package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.SourceIngestionService;
import org.neuinfo.foundry.common.model.PrimaryKeyDef;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.MongoUtils;
import org.neuinfo.foundry.common.util.Utils;

/**
 * Created by bozyurt on 2/4/16.
 */
public class PrimaryKeyDefTests extends TestCase {

    public PrimaryKeyDefTests(String name) {
        super(name);
    }

    public void testPK() throws Exception{
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");
            // biocaddie-0011 dryad
            BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", "biocaddie-0011");
            DB db =  helper.getDB();
            DBCollection sources = db.getCollection("sources");
            Source source = MongoUtils.getSource(query, sources);
            assertNotNull(source);
            PrimaryKeyDef keyDef = source.getPrimaryKeyDef();
            String homeDir = System.getProperty("user.home");
            String jsonFile = homeDir + "/dev/java/Foundry-Data/SampleData/dryad/dryad_oai_record.json";
            String jsonStr = Utils.loadAsString(jsonFile);
            JSONObject origData = new JSONObject(jsonStr);
            String primaryKey = keyDef.prepPrimaryKey(origData);
            assertNotNull(primaryKey);
            System.out.println(primaryKey);

        } finally {
            helper.shutdown();
        }
    }
}
