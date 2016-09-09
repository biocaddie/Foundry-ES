package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.coordinator.JavaPluginCoordinator;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

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

    public void testBiocaddieNLPEnhancer() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            DocumentIngestionService dis = new DocumentIngestionService();
            dis.start(helper.getConfig());

            JavaPluginCoordinator.getInstance(helper.getConfig().getPluginDir(),
                    helper.getConfig().getLibDir());
            List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0022", 10);
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
