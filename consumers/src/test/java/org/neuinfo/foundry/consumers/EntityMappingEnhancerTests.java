package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.EntityMappingEnhancer;

/**
 * Created by bozyurt on 3/9/16.
 */
public class EntityMappingEnhancerTests extends TestCase {
    public EntityMappingEnhancerTests(String name) {
        super(name);
    }

    public void testAddEntityMappings() throws Exception {
        EntityMappingEnhancer eme = new EntityMappingEnhancer();
        String jsonStr = Utils.loadTextFromClasspath("entity_mapping_test.json");
        assertFalse(jsonStr.length() == 0);
        JSONObject trJSON = new JSONObject(jsonStr);
        String srcID = "nif-0000-00006";
        eme.addEntityMappings(srcID, trJSON);

        System.out.println(trJSON.toString(2));
    }
}
