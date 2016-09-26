package org.neuinfo.foundry.common.transform;

import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 9/20/16.
 */
public class TransformationEngineTest {


    @Test
    public void testConditionalConstantTransform() throws Exception {
        String jsonStr = "{\"dc:eddi_accessurl\": {\"_$\": \"http://physionet.org/physiobank/database/aami-ec13/\"}}";
        String transformScript = "if \"$.'dc:eddi_accessurl'.'_$'\" like \"%physionet%\" then let \"datasetDistributions[0].storedIn\" = \"PhysioNet\";";
        TransformationEngine trEngine = new TransformationEngine(transformScript);
        JSONObject transformedJson = new JSONObject();
        JSONObject json = new JSONObject(jsonStr);
        trEngine.transform(json, transformedJson);
        assertTrue(transformedJson.has("datasetDistributions"));
        JSONObject elemJson = transformedJson.getJSONArray("datasetDistributions").getJSONObject(0);
        assertEquals("PhysioNet", elemJson.getString("storedIn"));
        System.out.println(transformedJson.toString(2));
    }

    @Test
    public void testArray2One() throws Exception {
        String jsonStr = "{\"useStmt\": {" +
                "\"conditions\": {\"p\": [" +
                "{\"_$\": \"Terms of use are available at http://www.icpsr.umich.edu/icpsrweb/ICPSR/studies/2/terms\"}," +
                "{\"_$\": \"AVAILABLE. This study is freely available to ICPSR member institutions.\"}" +
                "]}," +
                "\"disclaimer\": {\"_$\": \"The original collector of the data, ICPSR, and the relevant funding agency bear no responsibility for use of the data or for interpretations or inferences based upon such uses.\"}" +
                "}}}";
        JSONObject json = new JSONObject(jsonStr);

        String transformScript = "join \"$.'useStmt'.'conditions'.'p'[*].'_$'\" to \"datasetDistribution[0].license\" apply {{  result=' '.join(value) }};";
        TransformationEngine trEngine = new TransformationEngine(transformScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);
        System.out.println(transformedJson.toString(2));
    }
}