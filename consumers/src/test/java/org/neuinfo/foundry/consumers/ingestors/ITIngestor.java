package org.neuinfo.foundry.consumers.ingestors;

import org.junit.Test;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.*;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 4/3/15.
 */
public class ITIngestor {

    static String FLYSTOCK_TR_SCRIPT =
            "transform columns \"Donor\",\"Donor's source\" to \"Donor's Source\" apply {{result=value1 if not value2 else value2}};" +
                    "transform columns \"Donor\",\"Donor's source\" to \"Donor\" apply {{result=value2 if not value2 and not value1 else ''}};" +
                    "transform column \"Stk #\" to \"ID\" apply {{result='BDSC:' + value}};" +
                    "transform column \"Stk #\" to \"CatalogID\";" +
                    "transform column \"Stk #\" to \"Stocknumber\";" +
                    "transform column \"Stk #\" to \"URL\" apply {{result='http://flystocks.bio.indiana.edu/Reports/' + value + '.html'}};" +
                    "transform column \"Breakpts/Insertion\" to \"breakpoints\";" +
                    "transform column \"Stk #\" to \"Species\" apply {{result='Fruit Fly'}};" +
                    "transform column \"Genotype\" to \"Name\";" +
                    "transform column \"Comments\" to \"Notes\";" +
                    "transform column \"Date added\" to \"Date Added\";" +
                    "transform column \"Stk #\" to \"CURIE\" apply {{result='BDSC:' + value}};" +
                    "transform column \"Ch # all\" to \"Background\" apply {{result='Wild Type' if value.lower() == 'wt' else 'Mutant'}};" +
                    "transform column \"Ch # all\" to \"Affected Gene\" apply {{result='Chromosome:' + 'NULL' if value.lower() == 'wt' else value}};";


    @Test
    public void testIngestCSV() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://flystocks.bio.indiana.edu/bloomington.csv");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        options.put("transformScript", FLYSTOCK_TR_SCRIPT);

        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);

            count++;
            if ((count % 100) == 0) {
                System.out.println("Handled so far:" + count);
            }
            // FileUtils.saveText(result.getPayload().toString(2), "/tmp/flystocks_csv_record.json", CharSetEncoding.UTF8);
        }
    }

    public void testIngestFromResourceAsCSV() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("sourceURL", "ds:biocaddie-0004::HMS Dataset ID");
        options.put("urlTemplate", "http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv");
        options.put("fileType", "csv");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        options.put("fieldsToAdd", "HMS Dataset ID:hmsDatasetID");
        options.put("sampleMode", "true");

        ServiceFactory.getInstance("consumers-cfg.xml");
        ResourceIngestor ingestor = new ResourceIngestor();

        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        File dir = new File("/tmp/lincs_ds_result");
        dir.mkdir();
        while (ingestor.hasNext()) {
            count++;
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);

            Utils.saveText(result.getPayload().toString(2),
                    "/tmp/lincs_ds_result/lincs_ds_result_" + count + ".json");
            if ((count % 10) == 0) {
                System.out.println("ingested so far " + count);
            }
        }
    }

    public void testIngestPDBFromFTP() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.wwpdb.org");
        options.put("remotePath", "/pub/pdb/data/structures/divided/XML/");
        options.put("filenamePattern", ".+\\.xml\\.gz$");
        options.put("recursive", "true");
        //  options.put("outDir", "/tmp/pdb_ftp");
        options.put("maxDocs", "10");

        options.put("documentElement", "datablock");


        String transformScript = TransformMappingUtils.loadTransformMappingScriptFromClasspath("transform/pdb.trs");
        assertNotNull(transformScript);
        assertTrue(!transformScript.isEmpty());
        System.out.println(transformScript);
        options.put("transformScript", transformScript);

        FTPIngestor ingestor = new FTPIngestor();
        ingestor.setTestMode(true);
        ingestor.initialize(options);
        ingestor.startup();
        if (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            Utils.saveText(result.getPayload().toString(2), "/tmp/pdb_xml_record.json");
        }
    }

    public void testIngestMGI() throws Exception {
        // original data from postgres db
        // uses raw tables and views not in CM (in disco dv)
        String tableNames = "l2_nif_0000_00096_mgi_gxd_genotype_summary_view a,";
        String joinInfo = "";
    }


    public void testIngestAR_Antibodies() throws Exception {
        String tableNames = "antibody_table";
        int sampleSize = 5;
        String outFile = "/tmp/ar_antibodies_record.json";
        String joinInfoStr = null;
        ingestSampleFromMysql(tableNames, sampleSize, outFile, joinInfoStr);
    }

    public void testIngestScicrunchRegistry() throws Exception {
        String tableNames = "scicrunch_registry_view";
        String outFile = "/tmp/scicrunch_registry_record.json";
        String joinInfoStr = null;
        ingestSampleFromMysql(tableNames, 5, outFile, joinInfoStr);
    }

    private void ingestSampleFromMysql(String tableNames, int sampleSize, String outFile, String joinInfoStr) throws Exception {
        Parameters parameters = Parameters.getInstance();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("jdbcURL", parameters.getParam("mysql.dbURL"));
        options.put("dbUser", parameters.getParam("mysql.user"));
        options.put("dbPassword", parameters.getParam("mysql.password"));
        options.put("tableNames", tableNames);
        if (joinInfoStr != null) {
            options.put("joinInfo", joinInfoStr);
        }
        DISCOIngestor ingestor = new DISCOIngestor();
        ingestor.initialize(options);
        TestUtils.ingest(ingestor, outFile, sampleSize);
    }


    public void testIngestFromAllenXml() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://api.brain-map.org/api/v2/data/query.xml?criteria=model::Specimen,rma::criteria,[is_cell_specimen$eq%27true%27],rma::options[num_rows$eqall],rma::include,structure,donor%28transgenic_lines%29,ephys_features,specimen_tags,neuron_reconstructions");
        options.put("topElement", "specimens");
        options.put("documentElement", "specimen");

        NIFXMLIngestor ingestor = new NIFXMLIngestor();
        ingestor.initialize(options);
        TestUtils.ingest(ingestor, "/tmp/allen_record.json", 5);
    }


}
