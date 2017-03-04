package org.neuinfo.foundry.common.transform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.neuinfo.foundry.common.util.Utils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

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


    @Test
    public void testPythonStringIssue() throws Exception {
        URL url = TransformationEngineTest.class.getClassLoader().getResource("testdata/nursa_errors.json");
        String path = url.toURI().getPath();
        System.out.println(path);
        String jsonStr = Utils.loadAsString(path);
        JSONArray jsArr = new JSONArray(jsonStr);
        JSONObject json = jsArr.getJSONObject(1).getJSONObject("OriginalDoc");
        String transformationScript = loadAsStringFromClassPath("testdata/nursa_datasets.trs");
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
    }

    public void testLSDBIssue() throws Exception {
        URL url = TransformationEngineTest.class.getClassLoader().getResource("testdata/all_data_LSDB.json");
        String path = url.toURI().getPath();
        System.out.println(path);
        String jsonStr = Utils.loadAsString(path);
        JSONObject root = new JSONObject(jsonStr);
        JSONArray jsArr = root.getJSONArray("data");

        String transformationScript = loadAsStringFromClassPath("testdata/LSDB.trs");
        TransformationEngine trEngine = new TransformationEngine(transformationScript);

        for (int i = 0; i < jsArr.length(); i++) {
            JSONObject transformedJson = new JSONObject();
            JSONObject json = jsArr.getJSONObject(i);
            trEngine.transform(json, transformedJson);

            System.out.println(transformedJson.toString(2));
            System.out.println("-------------------------------");
        }
    }

    @Test
    public void testNatureIssue() throws Exception {
        URL url = TransformationEngineTest.class.getClassLoader().getResource("testdata/nature_record_17.json");
        String path = url.toURI().getPath();
        System.out.println(path);
        String jsonStr = Utils.loadAsString(path);
        JSONObject json = new JSONObject(jsonStr);
        String transformationScript = loadAsStringFromClassPath("testdata/nature_test.trs");
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
        System.out.println("-------------------------------");
    }

    @Test
    public void testJoinMulti() throws Exception {
        String jsonStr = loadAsStringFromClassPath("testdata/uniprot_swissprot_record.json");
        String transformationScript = loadAsStringFromClassPath("testdata/uniport_swissprot.trs");
        JSONObject json = new JSONObject(jsonStr);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
        assertTrue(transformedJson.getJSONArray("taxonomicInformation").getJSONObject(0).has("strain"));
    }

    @Test
    public void testTransformColumns() throws Exception {
        String jsonStr = loadAsStringFromClassPath("testdata/openfmri_record_1.json");
        String transformationScript = loadAsStringFromClassPath("testdata/openfmri.trs");
        JSONObject json = new JSONObject(jsonStr);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
    }


    @Test
    public void testSwissprotIssue() throws Exception {
        String jsonStr = loadAsStringFromClassPath("testdata/swissprot_errors.json");
        String transformationScript = loadAsStringFromClassPath("testdata/uniprot_swissprot.trs");
        JSONArray jsArr = new JSONArray(jsonStr);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        for (int i = 0; i < jsArr.length(); i++) {
            System.out.println("=============================");
            JSONObject json = jsArr.getJSONObject(i);

            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);

            System.out.println(transformedJson.toString(2));

        }
    }

    @Test
    public void testSample1() throws Exception {
        String jsonStr = loadAsStringFromClassPath("testdata/sample1.json");
        String transformationScript = loadAsStringFromClassPath("testdata/sample1.trs");
        JSONObject json = new JSONObject(jsonStr);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
    }

    @Test
    public void testLSDB() throws Exception {
        String jsonStr = loadAsStringFromClassPath("testdata/all_data_LSDB.json");
        String transformationScript = loadAsStringFromClassPath("testdata/transformation_script_LSDB.trs");
        // String transformationScript = loadAsStringFromClassPath("testdata/lsdb_test.trs");
        JSONObject json = new JSONObject(jsonStr);
        JSONArray jsArr = json.getJSONArray("data");
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        for (int i = 0; i < jsArr.length(); i++) {
            System.out.println("=============================");
            json = jsArr.getJSONObject(i);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);

            System.out.println(transformedJson.toString(2));
        }
    }

    @Test
    public void testMissingIfColumn() throws Exception {
        JSONObject json = new JSONObject(); // empty data
        String transformationScript = loadAsStringFromClassPath("testdata/pubmed_test.trs");
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
    }

    public static String loadAsStringFromClassPath(String classpath) throws Exception {
        URL url = TransformationEngineTest.class.getClassLoader().getResource(classpath);
        String path = url.toURI().getPath();
        return Utils.loadAsString(path);
    }
}