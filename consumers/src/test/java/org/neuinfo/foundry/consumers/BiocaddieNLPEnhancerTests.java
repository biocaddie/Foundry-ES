package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.coordinator.JavaPluginCoordinator;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 8/9/16.
 */
public class BiocaddieNLPEnhancerTests extends TestCase {
    public BiocaddieNLPEnhancerTests(String name) {
        super(name);
    }

    public void testBiocaddieNLPEnhancerFromFile() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            DocumentIngestionService dis = new DocumentIngestionService();
            dis.start(helper.getConfig());

            JavaPluginCoordinator.getInstance(helper.getConfig().getPluginDir(),
                    helper.getConfig().getLibDir());
            List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0027", 1);
            String pluginClass = "edu.uth.biocaddie.ner.BiocaddieNLPEnhancer";

            Map<String, String> options = new HashMap<String, String>(3);
            IPlugin plugin = (IPlugin) JavaPluginCoordinator.getInstance().createInstance(pluginClass);
            System.out.println(plugin.getPluginName());

            plugin.initialize(options);

            for (BasicDBObject docWrapper : docWrappers) {
                BasicDBObject data = (BasicDBObject) docWrapper.get("Data");

                URL url = BiocaddieNLPEnhancerTests.class.getClassLoader().getResource("testdata/geo_error_tr.json");
                String path = url.toURI().getPath();
                System.out.println(path);
                String jsonStr = Utils.loadAsString(path);
                BasicDBObject trDBO = (BasicDBObject) JSONUtils.encode(new JSONObject(jsonStr));
                data.put("transformedRec", trDBO);
                System.out.println(JSONUtils.toJSON(trDBO, false).toString(2));
                System.out.println("==================");
                Result result = plugin.handle(docWrapper);

                assertTrue(result.getStatus() == Result.Status.OK_WITHOUT_CHANGE);
                DBObject dw = result.getDocWrapper();

                data = (BasicDBObject) dw.get("Data");

                System.out.println("Processed Data");
                System.out.println("--------------");
                System.out.println(JSONUtils.toJSON(data, false).toString(2));
                break;
            }

        } finally {
            helper.shutdown();
        }
    }

    public void testBiocaddieNLPEnhancer() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            DocumentIngestionService dis = new DocumentIngestionService();
            dis.start(helper.getConfig());

            JavaPluginCoordinator.getInstance(helper.getConfig().getPluginDir(),
                    helper.getConfig().getLibDir());
            // List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0022", 10);
            List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0027", 10);
            String pluginClass = "edu.uth.biocaddie.ner.BiocaddieNLPEnhancer";

            Map<String, String> options = new HashMap<String, String>(3);
            IPlugin plugin = (IPlugin) JavaPluginCoordinator.getInstance().createInstance(pluginClass);

            //   IPlugin plugin = new BiocaddieNLPEnhancer();
            System.out.println(plugin.getPluginName());

            plugin.initialize(options);

            for (BasicDBObject docWrapper : docWrappers) {
                BasicDBObject procDBO = (BasicDBObject) docWrapper.get("Processing");
                String status = (String) procDBO.get("status");
                if (status.equals("transformed.1")) {
                    BasicDBObject data = (BasicDBObject) docWrapper.get("Data");

                    BasicDBObject trDBO = (BasicDBObject) data.get("transformedRec");
                    System.out.println(JSONUtils.toJSON(trDBO, false).toString(2));
                    System.out.println("==================");
                    Result result = plugin.handle(docWrapper);

                    DBObject dw = result.getDocWrapper();

                    data = (BasicDBObject) dw.get("Data");

                    System.out.println("Processed Data");
                    System.out.println("--------------");
                    System.out.println(JSONUtils.toJSON(data, false).toString(2));
                    break;
                }
            }


        } finally {
            helper.shutdown();
        }

    }
}
