package org.neuinfo.foundry.common.util;

import org.jdom2.Element;
import org.json.JSONObject;
import org.junit.Test;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.transform.TransformationFunctionRegistry;

import java.io.File;
import java.io.IOException;

/**
 * Created by bozyurt on 5/11/15.
 */

public class ITTransformEngineProcessingTests {
    final static String HOME_DIR = System.getProperty("user.home");

    @Test
    public void testPDBTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/pdb/100d-noatom.json";
        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-Data/transformations/pdb.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    @Test
    public void testGemmaTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/Gemma/gemma_csv_record_1.json";
        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-Data/transformations/gemma.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testBioprojectTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/Bioproject/bioproject_sample1.json";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/bioproject.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testDryadTransformation() throws Exception {
        TransformationFunctionRegistry tfRegistry = TransformationFunctionRegistry.getInstance();
        tfRegistry.registerFunction("toStandardDate",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTransformation"));
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/dryad/dryad_oai_record.json";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/dryad.trs";
        // transformScriptFile = "/tmp/dryad.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }


    public void testClinicalTrialsTransformation() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/java/Foundry-Data/SampleData/clinicaltrials.gov/clinicaltrials_gov_record.json";
        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-Data/transformations/clinicaltrials.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    @Test
    public void testNeuroMorphoTransformation() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/java/Foundry-Data/SampleData/neuromorpho/neuromorpho_record_1.json";
        //String transformScriptFile = "/tmp/neuromorpho.trs";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/neuromorpho.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testCIATransformation() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/SampleData/CancerImagingArchive/cia_record_1.json";
        //String transformScriptFile = "/tmp/cia.trs";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/cancer_imaging_archive.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    @Test
    public void testPDBTransformation2() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/java/Foundry-Data/SampleData/pdb/5amh-noatom.json";
        //     String transformScriptFile = "/tmp/pdb.trs";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/pdb.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testNeuroMorphoTransformation1() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/java/Foundry-Data/SampleData/neuromorpho/neuromorpho_record_1.json";
        String transformScriptFile = HOME_DIR +
                "/dev/biocaddie/data-pipeline/transformations/neuromorpho_test.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testPDBAccessURLBug() throws Exception {
        String inJsonFile = HOME_DIR +
                "/dev/java/Foundry-Data/SampleData/pdb/5amh-noatom.json";
        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-ES/test/pdb_test.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public static void handleTransformation(String inJsonFile, String transformScriptFile) throws IOException {
        JSONObject json = new JSONObject(Utils.loadAsString(inJsonFile));
        String transformScript = TransformMappingUtils.loadTransformMappingScript(transformScriptFile);
        TransformationEngine trEngine = new TransformationEngine(transformScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);
        System.out.println(transformedJson.toString(2));
    }

    public void testICGDynamic() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/KnowledgeSpace/Ion_Channel_Geneology/icg_record_1.json";
        // String transformScriptFile = "/tmp/icg.trs";
        String transformScriptFile = HOME_DIR + "/dev/java/Foundry-Data/transformations/KnowledgeSpace/icg.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testAssignNameFromClause() throws Exception {
        JSONObject sourceJSON = new JSONObject();
        sourceJSON.put("measurementType", "temperature");
        sourceJSON.put("measurementValue", 310);
        System.out.println(sourceJSON.toString(2));
        System.out.println("=======================");
        String transformationScript = "transform column \"$.measurementValue\" to \"measurements.name\" "
                + "assign name from \"$.measurementType\";";
        System.out.println(transformationScript);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(sourceJSON, transformedJson);
        System.out.println(transformedJson.toString(2));
    }

    public void testSRATransformation() throws Exception {

        String inJsonFile = "/tmp/sra/sra_sample_record_138.json";
        /*
        JSONObject sourceJSON = new JSONObject(Utils.loadAsString(inJsonFile));
        String transformationScript =
                "transform column \"$.'SRA'.'SAMPLE_SET'.'SAMPLE'[*].'@alias'\" to \"SRA.SAMPLE_SET.SAMPLE[].@alias\";"
                        + "transform column \"$.'SRA'.'SAMPLE_SET'.'SAMPLE'[*].'@accession'\" to \"SRA.SAMPLE_SET.SAMPLE[].@accession\";";
        //  transformationScript =
        //          "transform column \"$.'SRA'.'STUDY_SET'.'STUDY'.'IDENTIFIERS'.'EXTERNAL_ID'.'_$'\" to \"SRA.STUDY_SET.STUDY.IDENTIFIERS.EXTERNAL_ID\";";
        transformationScript =
                "transform column \"$.'SRA'.'STUDY_SET'.'STUDY'.'IDENTIFIERS'.'EXTERNAL_ID'.'@namespace'\" to \"SRA.STUDY_SET.STUDY.IDENTIFIERS.EXTERNAL_ID.@namespace\";";
        System.out.println(transformationScript);
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(sourceJSON, transformedJson);
        System.out.println(transformedJson.toString(2));
        */

        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-Data/transformations/sra.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testCVRGTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/CVRG/cvrg_sample_record_1.json";
        String transformScriptFile = HOME_DIR + "/dev/java/Foundry-Data/transformations/cvrg.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testDataverseTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/dataverse/dataverse_sample_record_1.json";
        String transformScriptFile = HOME_DIR + "/dev/java/Foundry-Data/transformations/dataverse.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testDataverse2Transformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/dataverse2/dataverse2_record_1.json";
        String transformScriptFile = HOME_DIR + "/dev/java/Foundry-Data/transformations/dataverse2.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testArrayExpressTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/ArrayExpress/array_express_sample_1.json";
        String transformScriptFile = HOME_DIR +
                "/dev/java/Foundry-Data/transformations/arrayexpress.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }

    public void testGEOTransformation() throws Exception {
        TransformationFunctionRegistry tfRegistry = TransformationFunctionRegistry.getInstance();

        tfRegistry.registerFunction("toStandardDate",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTransformation"));
        String inJsonFile = "/tmp/geo_datasets_soft_record.json";
        JSONObject json = new JSONObject(Utils.loadAsString(inJsonFile));
        String transformScript = TransformMappingUtils.loadTransformMappingScript(HOME_DIR +
                "/dev/java/Foundry-Data/transformations/geo_dataset.trs");
        TransformationEngine trEngine = new TransformationEngine(transformScript);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(json, transformedJson);
        System.out.println(transformedJson.toString(2));
    }

    public void testConvertPDBXML() throws Exception {
        String pdbFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/pdb/100d-noatom.xml";
        Element rootEl = Utils.loadXML(pdbFile);
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        Utils.saveText(json.toString(2), HOME_DIR + "/dev/java/Foundry-Data/SampleData/pdb/100d-noatom.json");
    }

    public void testArrayResult() throws Exception {

        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        //System.out.println("Press a key to continue:");
        Profiler profiler = Profiler.getInstance("tr");
        // File pdbFile = new File("/tmp/pdb_ftp/04/104d.xml");
        File pdbFile = new File("/var/temp/pdb_rsync/za/1za0-noatom.xml");
        profiler.entryPoint("loadXML");
        Element rootEl = Utils.loadXML(pdbFile.getAbsolutePath());
        profiler.exitPoint("loadXML");
        profiler.entryPoint("converter");
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        profiler.exitPoint("converter");
        profiler.entryPoint("loadScript");
        String transformScript = TransformMappingUtils.loadTransformMappingScriptFromClasspath("transform/pdb.trs");
        profiler.exitPoint("loadScript");
        TransformationEngine trEngine = new TransformationEngine(transformScript);
        JSONObject transformedJson = new JSONObject();
        profiler.entryPoint("src/test/resources/transform");
        trEngine.transform(json, transformedJson);
        profiler.exitPoint("src/test/resources/transform");
        System.out.println(transformedJson.toString(2));

        profiler.showStats();
    }

    public void testConditionalTransformation() throws Exception {
        String inJsonFile = HOME_DIR + "/dev/java/Foundry-ES/test/cvrg_sample_record_1.json";
        String transformScriptFile = HOME_DIR + "/dev/java/Foundry-ES/test/cvrg_test.trs";
        handleTransformation(inJsonFile, transformScriptFile);
    }


}
