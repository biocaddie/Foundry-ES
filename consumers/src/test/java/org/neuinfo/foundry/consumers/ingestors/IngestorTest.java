package org.neuinfo.foundry.consumers.ingestors;

import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 4/3/15.
 */
public class IngestorTest extends TestCase {
    public IngestorTest(String name) {
        super(name);
    }

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

    public void testTwoStageJSONIngestor() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("idIngestURL",
                "https://dataverse.harvard.edu/api/search?key=c856dcf1-7df3-4bbc-82e8-311157d11281&q=*&type=dataset&show_entity_ids=true");
        options.put("idJsonPath", "$..entity_id");
        options.put("totalParamJsonPath", "$.data.total_count");
        options.put("offsetParam", "start");
        options.put("dataIngestURLTemplate",
                "https://dataverse.harvard.edu/api/datasets/${id}?key=c856dcf1-7df3-4bbc-82e8-311157d11281");
        options.put("docJsonPath", "$.data");
        TwoStageJSONIngestor ingestor = new TwoStageJSONIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/dataverse2_record.json", 5);
    }

    public void testIngestIonChannelGen() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("idIngestURLTemplate",
                "http://icg.neurotheory.ox.ac.uk/api/chs/${familyId}/");
        options.put("idIngestURLTemplateParams", "1971,1972,1973,1973,1974,1975");
        options.put("parentDocJsonPath", "$.chans");
        options.put("idJsonPath", "$.id");
        options.put("dataIngestURLTemplate",
                "http://icg.neurotheory.ox.ac.uk/api/chs/detail/${id}/");
        options.put("childDocParentIDPath", "familyNumbers[]");
        TwoStageJSONIngestor ingestor = new TwoStageJSONIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/icg_record.json", 5);
    }

    public void testIngestCSV1() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/cells/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_cells_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, -1);
        assertTrue(count > 900);
    }

    public void testIngestCSV2() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/sm/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_sm_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, -1);
        System.out.println("count=" + count);
        assertTrue(count >= 427);
    }

    public void testIngestCSV3() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/datasets/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_ds_summary_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, -1);
        System.out.println("count=" + count);
        assertTrue(count >= 207);
    }

    public void testIngestGemmaCSV() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL",
                "http://www.chibi.ubc.ca/Gemma/datasetdownload/4.20.2011/DatasetSummary.view.txt.gz");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", "\t");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        String outFilePrefix = "/tmp/gemma_csv_record_";

        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        boolean first = true;

        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            System.out.println(result.getPayload().toString(2));
            System.out.println("======================");
            count++;
            if ((count % 100) == 0) {
                System.out.println("Handled so far:" + count);
            }
            if (count < 5) {
                String outFile = outFilePrefix + count + ".json";
                Utils.saveText(result.getPayload().toString(2), outFile);
                first = false;
            }
            if (count > 1000) {
                break;
            }
        }
        System.out.println("count=" + count);
    }


    private int ingestCSV(String ingestURL, String outFile, int sampleSize) throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", ingestURL);
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");

        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            System.out.println(result.getPayload().toString(2));
            System.out.println("======================");
            String jsonFile = outFile.replaceFirst("\\.json$", "_" + (count + 1) + ".json");
            Utils.saveText(result.getPayload().toString(2), jsonFile);
            System.out.println("wrote " + jsonFile);

            count++;
            if ((count % 100) == 0) {
                System.out.println("Handled so far:" + count);
            }
            if (sampleSize > 0 && count >= sampleSize) {
                break;
            }
        }
        return count;
    }

    public void testSRAIngest() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("rsyncSource", "ftp.ncbi.nlm.nih.gov::sra/Submissions/");
        options.put("rsyncDest", "/var/data/foundry-es/cache/data/SRA");

        options.put("filenamePattern", ".+\\.xml$");
        options.put("documentElement", "SRA");
        options.put("fullSet", "true");
        options.put("rsyncIncludePattern", "*.xml");
        options.put("preprocessCommand", "combine");
        options.put("surroundingTag", "SRA");
        options.put("testMode", "true");
        RsyncIngestor ingestor = new RsyncIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/sra/sra_sample_record.json", 500);
    }

    public void testDbGaPIngest() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("rsyncSource", "ftp.ncbi.nlm.nih.gov::dbgap/studies/phs000001/");
        options.put("rsyncDest", "/var/data/foundry-es/cache/data/dbGaP");

        options.put("filenamePattern", "GapExchange_.+\\.xml$");
        options.put("topElement", "Studies");
        options.put("documentElement", "Study");
        options.put("fullSet", "true");
        options.put("rsyncIncludePattern", "*.xml");
        RsyncIngestor ingestor = new RsyncIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/dbGaP_sample_record.json", 500);
    }


    public void testIngestBDSC() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://flystocks.bio.indiana.edu/bloomington.csv");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/bdsc_record.json", 5);
    }

    public void testIngestBlueBrain_Bluima_Connectivity() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://data.neuinfo.org/hbp/20140226_aba_data-mbafix.csv");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/bluebrain_bc_record.json", 5);
    }

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

    public void testIngestByAspera() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("filenamePattern", "\\.soft\\.gz$");
        options.put("documentElement", "MINiML");
        options.put("source", "anonftp@ftp.ncbi.nlm.nih.gov:/geo/datasets/");
        options.put("dest", "/var/data/geo");
        options.put("publicKeyFile", "/home/bozyurt/.aspera/connect/etc/asperaweb_id_dsa.openssh");
        options.put("arguments", "-k1 -Tr -l200m");
        options.put("fullSet", "true");
        options.put("xmlFileNamePattern", "\\.xml$");
        options.put("parserType", "geo");

        AsperaIngestor ingestor = new AsperaIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        if (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            Utils.saveText(result.getPayload().toString(2), "/tmp/geo_datasets_soft_record.json");
        }
        ingestor.shutdown();
    }

    public void testIngestXMLFromFTP() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.ebi.ac.uk");
        options.put("outDir", "/var/data/foundry-es/test");
        options.put("filenamePattern", "MODEL\\d+\\.xml");
        options.put("documentElement", "sbml");
        options.put("pathPattern",
                "/pub/databases/biomodels/weekly_archives/%[\\d{4,4}]%/BioModels-Database-weekly-%[\\d+\\-\\d\\d\\-\\d\\d]%-sbmls.tar.bz2");
        options.put("pattern1Type", "date_yyyy");
        options.put("pattern2Type", "date_yyyy-MM-dd");
        FTPIngestor ingestor = new FTPIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        if (ingestor.hasNext()) {
            processPayload(ingestor, "/tmp/biomodels_xml_record.json");
        }
    }

    public void testIngestBioprojectFromFTP() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.ncbi.nlm.nih.gov");
        options.put("outDir", "/var/data/foundry-es/cache/data");
        options.put("documentElement", "Package");
        options.put("topElement", "PackageSet");
        options.put("remotePath", "bioproject/bioproject.xml");

        FTPIngestor ingestor = new FTPIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            String suffix = count == 0 ? "" : String.valueOf(count);
            Utils.saveText(result.getPayload().toString(2),
                    "/tmp/bioproject_sample" + suffix + ".json");
            count++;
            if (count == 10) {
                break;
            }
        }
    }

    public void testIngestRSS() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.wired.com/category/science/feed/");
        RSSIngestor ingestor = new RSSIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        if (ingestor.hasNext()) {
            processPayload(ingestor, "/tmp/wired_news_record.json");
        }
    }


    public void testIngestFromDisco1() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00006_1", "/tmp/neuromorpho_record.json", 5);
    }

    public void testIngestFromDisco2() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_37639_1", "/tmp/cell_image_library_record.json", 5);
    }

    public void testIngestFromDisco3() throws Exception {
        ingestSampleFromDisco("pr_nlx_151885_1", "/tmp/neuroelectro_record.json", 5);
    }

    public void testIngestFromDisco4() throws Exception {
        ingestSampleFromDisco("pr_scr_014194_1", "/tmp/ion_channel_geneology_record.json", 5);
    }

    public void testIngestFromDisco5() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00508_5", "/tmp/allen_cell_types_morphology_record.json", 5);
    }

    public void testIngestFromDisco6() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00508_4", "/tmp/allen_cell_types_electrophysiology_record.json", 5);
    }

    public void testIngestFromDisco7() throws Exception {
        ingestSampleFromDisco("pr_nlx_154697_8", "/tmp/integrated_connectivity_record.json", 5);
    }

    public void testIngestFromDisco8() throws Exception {
        // ingestSampleFromDisco("pr_nif_0000_03266_2", "/tmp/peptide_atlas_record.json", 5);
        ingestSampleFromDiscoWithTransform("pr_nif_0000_03266_2", "/tmp/peptide_atlas_record.json",
                5,"dvp","peptideatlas.trs");
    }

    public void testIngestFromDisco9() throws Exception {
        ingestSampleFromDisco("pr_nlx_144048_1", "/tmp/openfmri_record.json", 5);
    }

    public void testIngestFromDisco10() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_03160_1", "/tmp/mouse_phenome_db_record.json", 5);
    }

    public void testIngestFromDisco11() throws Exception {
        ingestSampleFromDisco("pr_nlx_48903_1", "/tmp/physiobank_record.json", 5);
    }

    public void testIngestFromDisco12() throws Exception {
        ingestSampleFromDisco("pr_nlx_158620_1", "/tmp/proteome_xchange_record.json", 5);
    }

    public void testIngestFromDisco13() throws Exception {
        ingestSampleFromDisco("pr_nlx_152660_1", "/tmp/yped_record.json", 5);
    }

    public void testIngestFromDisco15() throws Exception {
        ingestSampleFromDisco("pr_nlx_151749_1", "/tmp/cia_record.json", 5, "dv");
    }

    public void testIngestFromDisco16() throws Exception {
        // ingestSampleFromDisco("pr_nif_0000_03208_2", "/tmp/nursa_datasets_record.json", 5);
        ingestSampleFromDisco("nif_0000_03208_2", "/tmp/nursa_datasets_record.json", 5, "dv");
    }

    public void testIngestFromDisco17() throws Exception {
        ingestSampleFromDisco("pr_nlx_152673_1", "/tmp/niddk_central_rep_record.json", 5);
    }

    public void testIngestFromDisco18() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_21981_1", "/tmp/clinical_trials_network_record.json", 5);
    }

    public void testIngestNeuroelectro() throws Exception {
        ingestSampleFromDiscoRaw("l2_nlx_151885_data_summary", null, "/tmp/neuroelectro_record.json", 5);
    }

    public void testIngestIntegratedConnectivity() throws Exception {
        ingestSampleFromDisco("pr_nlx_154697_8", "/tmp/integrated_connectivity_record.json", 5);
    }

    public void testIngestRGD() throws Exception {
        String tableNames = "l2_nif_0000_00134_data_rattus_strains_mp a,l2_nlx_83784_data_term b, l2_nif_0000_00134_data_strain_detail c";
        String joinInfo = "a.go_id=b.id, a.db_object_id=c.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/RGD_record.json", 5);
    }

    public void testIngestCellosaurus() throws Exception {
        String tableNames = "l2_scr_013869_cellosaurus_cellosaurus";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/cellosaurus_record.json", 5);
    }

    public void testIngestBrainMaps() throws Exception {
        String tableNames = "l2_nif_0000_00093_brainmaps_brainimage a, l2_nif_0000_00093_brainmaps_dataset b";
        String joinInfo = "a.id = b.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/brainmaps_record.json", 5);
    }

    public void testIngestCocomac() throws Exception {
        String tableNames = "l2_nif_0000_00022_data_labelledsites_data a, l2_nif_0000_00022_data_labelledsites_descriptions b, " +
                "l2_nif_0000_00022_data_injections c, " +
                "l2_nif_0000_00022_data_brainmaps_brainsites d," +
                "l2_nif_0000_00022_data_brainmaps_brainsiteacronyms e, " +
                "l2_nif_0000_00022_data_methods f, " +
                "l2_nif_0000_00022_data_literature_journalarticles g";
        String joinInfo = "a.id_description = b.id, b.id_injection = c.id, " +
                "a.id_brainsite = d.id, " +
                "c.id_method = f.id, " +
                "d.id_brainmaps_brainsiteacronym = e.id," +
                "f.id_literature = g.id_literature";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/cocomac_record.json", 5);
    }

    public void testIngestConnectomeWiki() throws Exception {
        // nif-0000-24441
        String tableNames = "l2_nif_0000_24441_CONNECTOMEWIKI_NAME a," +
                "l2_nif_0000_24441_CONNECTOMEWIKI_FROM b, " +
                "l2_nif_0000_24441_CONNECTOMEWIKI_NODE c," +
                "l2_nif_0000_24441_CONNECTOMEWIKI_TO d";
        String joinInfo = "a.term = b.z, b.x = c.x, b.x = d.x";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/connectome_wiki_record.json", 100);
    }

    public void testIngestMGI() throws Exception {
        // original data from postgres db
        // uses raw tables and views not in CM (in disco dv)
        String tableNames = "l2_nif_0000_00096_mgi_gxd_genotype_summary_view a,";
        String joinInfo = "";
    }

    public void testIngestFlybase() throws Exception {
        // original data from postgres db
        String tableNames = "l2_nif_0000_00558_fb_chado_stock a," +
                "l2_nif_0000_00558_fb_chado_organism b," +
                "l2_nif_0000_00558_fb_chado_stock_genotype c," +
                "l2_nif_0000_00558_fb_chado_genotype d";
        String joinInfo = "a.organism_id=b.organism_id, " +
                "a.stock_id=c.stock_id, c.genotype_id=d.genotype_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/flybase_record.json", 5);
    }

    public void testIngestBCBC() throws Exception {
        String tableNames = "l2_nlx_144143_adenovirus_list a," +
                "l2_nlx_144143_adenovirus_information b," +
                "l2_nlx_144143_adenovirus_publication c";
        String joinInfo = "a.id = b.id, a.id = c.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/bcbc_record.json", 100);
    }

    public void testIngestTemporalLobe() throws Exception {
        String tableNames = "l2_nif_0000_24805_data_list";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/temporal_lobe_record.json", 5);
    }

    public void testIngestNXR() throws Exception {
        String tableNames = "l2_scr_013731_xenopus_laevis";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/nxr_record.json", 5);
    }

    public void testIngestIMSR() throws Exception {
        String tableNames = "l2_nif_0000_09876_data_strain";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/imsr_record.json", 5);
    }

    public void testIngestZFIN() throws Exception {
        String tableNames = "l2_nif_0000_21427_data_genotypes a, " +
                "l2_nif_0000_21427_data_genotype_background b," +
                "l2_nif_0000_21427_data_wildtype_genotypes w";
        String joinInfo = "a.zfin_genotype_id = b.genotype_id, b.background_genotype_id = w.zfin_genotype_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/zfin_record.json", 100);
    }

    public void testIngestDGGR() throws Exception {
        String tableNames = "l2_nif_0000_30415_data_detail";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/dggr_record.json", 5);
    }

    public void testAntibodyRegistry() throws Exception {
        String tableNames = "l2_nif_0000_07730_nif_eelg_antibody_table a, " +
                "l2_nif_0000_07730_nif_eelg_antibody_vendor d," +
                "l2_nif_0000_07730_antibody_pmid p";
        String joinInfo = "a.vendor_id=d.id, a.id = p.ab_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/ar_record.json", 5);
    }

    public void testIngestWormbase() throws Exception {
        String tableNames = "l2_nif_0000_00053_strain_rrid_strain_data a";
        String joinInfo = null;
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/wormbase_record.json", 5);
    }

    public void testIngestZIRC() throws Exception {
        String tableNames = "l2_nif_0000_00242_catalog_line";
        String joinInfo = null;
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/zirc_record.json", 5);
    }

    public void testIngestTSC() throws Exception {
        String ingestURL = "https://tetrahymena.vet.cornell.edu/extras/TetrahymenaRRID.csv";
        String outFile = "/tmp/tsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void testIngestXGSC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/RRID_codes_Xiphophorus.csv";
        String outFile = "/tmp/xgsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void testIngestAGSC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/AmbystomaCatalog.csv";
        String outFile = "/tmp/agsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void testIngestNSRRC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/NSRRC_RRID.csv";
        String outFile = "/tmp/nsrrc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void testIngestAntibodies() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_07730_1", "/tmp/ar_antibodies_record.json", 5);
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
        ingest(ingestor, outFile, sampleSize);
    }

    public void ingestSampleFromDisco(String tableName, String outFile, int sampleSize) throws Exception {
        ingestSampleFromDisco(tableName, outFile, sampleSize, "dvp");
    }

    public void ingestSampleFromDisco(String tableName, String outFile, int sampleSize, String schemaName) throws Exception {
        Parameters parameters = Parameters.getInstance();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("jdbcURL", parameters.getParam("disco.dbURL"));
        options.put("dbUser", parameters.getParam("disco.user"));
        options.put("dbPassword", parameters.getParam("disco.password"));
        options.put("tableName", tableName);
        options.put("schemaName", schemaName);
        DISCOIngestor ingestor = new DISCOIngestor();
        ingestor.initialize(options);
        ingest(ingestor, outFile, sampleSize);
    }

    public void ingestSampleFromDiscoWithTransform(String tableName, String outFile, int sampleSize,
                                                   String schemaName, String transformScriptName) throws Exception {
        Parameters parameters = Parameters.getInstance();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("jdbcURL", parameters.getParam("disco.dbURL"));
        options.put("dbUser", parameters.getParam("disco.user"));
        options.put("dbPassword", parameters.getParam("disco.password"));
        options.put("tableName", tableName);
        options.put("schemaName", schemaName);
        DISCOIngestor ingestor = new DISCOIngestor();
        ingestor.initialize(options);
        ingestAndTransform(ingestor, outFile, sampleSize, transformScriptName);
    }

    public void ingestSampleFromDiscoRaw(String tableNames, String joinInfoStr, String outFile, int sampleSize) throws Exception {
        Parameters parameters = Parameters.getInstance();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("jdbcURL", parameters.getParam("disco.dbURL"));
        options.put("dbUser", parameters.getParam("disco.user"));
        options.put("dbPassword", parameters.getParam("disco.password"));
        options.put("tableNames", tableNames);
        if (joinInfoStr != null) {
            options.put("joinInfo", joinInfoStr);
        }
        options.put("fullRecordsOnly", "true");
        DISCOIngestor ingestor = new DISCOIngestor();
        ingestor.initialize(options);
        ingest(ingestor, outFile, sampleSize);
    }

    public void testIngestFromAllenXml() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://api.brain-map.org/api/v2/data/query.xml?criteria=model::Specimen,rma::criteria,[is_cell_specimen$eq%27true%27],rma::options[num_rows$eqall],rma::include,structure,donor%28transgenic_lines%29,ephys_features,specimen_tags,neuron_reconstructions");
        options.put("topElement", "specimens");
        options.put("documentElement", "specimen");

        NIFXMLIngestor ingestor = new NIFXMLIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/allen_record.json", 5);
    }

    private void ingest(Ingestor ingestor, String outFile, int sampleSize) throws Exception {
        try {
            ingestor.startup();
            int count = 0;
            while (ingestor.hasNext()) {
                String jsonFile = outFile.replaceFirst("\\.json$", "_" + (count + 1) + ".json");
                processPayload(ingestor, jsonFile);
                count++;
                if (count >= sampleSize) {
                    break;
                }
            }
        } finally {
            ingestor.shutdown();
        }
    }
    private void ingestAndTransform(Ingestor ingestor, String outFile, int sampleSize, String transformScriptName) throws Exception {
        try {
            ingestor.startup();
            String HOME_DIR = System.getProperty("user.home");
            String transformScript = TransformMappingUtils.loadTransformMappingScript(HOME_DIR +
                    "/dev/biocaddie/data-pipeline/transformations/" + transformScriptName);
            TransformationEngine trEngine = new TransformationEngine(transformScript);
            int count = 0;
            while (ingestor.hasNext()) {
                String jsonFile = outFile.replaceFirst("\\.json$", "_" + (count + 1) + ".json");
                processPayloadWithTransformation(ingestor, jsonFile, trEngine);
                count++;
                if (count >= sampleSize) {
                    break;
                }
            }
        } finally {
            ingestor.shutdown();
        }
    }
    public void testIngestOAI() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.datadryad.org/oai/request");
        options.put("sourceName", "Dryad");
        options.put("topElement", "ListRecords");
        options.put("documentElement", "record");
        // options.put("testMode", "true");
        OAIIngestor ingestor = new OAIIngestor();
        ingestor.initialize(options);
        ingestor.startup();

        if (ingestor.hasNext()) {
            processPayload(ingestor, "/tmp/dryad_oai_record.json");
        }
    }

    public void testCVRGOAI() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        //options.put("ingestURL", "https://eddi.cvrgrid.org/oai/request?verb=ListMetadataFormats");
        options.put("ingestURL", "https://eddi.cvrgrid.org/oai/request");
        options.put("sourceName", "CVRG");
        options.put("topElement", "ListRecords");
        options.put("documentElement", "record");
        options.put("metadataPrefix", "oai_dc_eddi");
        options.put("allowedSetSpecs", "col_11614_2,col_11614_1655");
        options.put("useCache", "false");
        OAIIngestor ingestor = new OAIIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/cvrg_sample_record.json", 5);
    }

    public void testICPSRGOAI() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies");
        options.put("sourceName", "ICPSR");
        options.put("topElement", "ListRecords");
        options.put("documentElement", "record");
        options.put("metadataPrefix", "oai_ddi25_citations");
        options.put("useCache", "false");
        OAIIngestor ingestor = new OAIIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/icpsr_sample_record.json", 5);
    }

    public void processPayload(Ingestor ingestor, String outFile) throws IOException {
        Result result = ingestor.prepPayload();
        assertNotNull(result);
        assertTrue(result.getStatus() != Result.Status.ERROR);
        Utils.saveText(result.getPayload().toString(2), outFile);
        System.out.println("saved file:" + outFile);
    }

    public void processPayloadWithTransformation(Ingestor ingestor, String outFile, TransformationEngine trEngine) throws IOException {
        Result result = ingestor.prepPayload();
        assertNotNull(result);
        assertTrue(result.getStatus() != Result.Status.ERROR);
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(result.getPayload(), transformedJson);
        System.out.println(transformedJson.toString(2));

        Utils.saveText(transformedJson.toString(2), outFile);
        System.out.println("saved file:" + outFile);

    }

    public void testIngestXml() throws Exception {
        // nlx_152590 OSB Projects
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.opensourcebrain.org/projects.xml");
        options.put("topElement", "projects");
        options.put("documentElement", "project");
        options.put("limitParam", "limit");
        options.put("limitValue", "100");
        options.put("offsetParam", "offset");

        NIFXMLIngestor ingestor = new NIFXMLIngestor();
        ingestor.initialize(options);

        ingestor.startup();

        boolean first = true;
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            if (first) {
                Utils.saveText(result.getPayload().toString(2), "/tmp/osb_record.json");
                first = false;
            }
            count++;
        }
        assertTrue(count > 0);
        System.out.println("# of xml records:" + count);
    }

    public void testIngestArrayExpress() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.ebi.ac.uk/arrayexpress/xml/v2/experiments");
        options.put("topElement", "experiments");
        options.put("documentElement", "experiment");

        NIFXMLIngestor ingestor = new NIFXMLIngestor();
        ingestor.initialize(options);

        ingest(ingestor,"/tmp/ae/array_express_record.json",5000);
    }

    public void testIngestDataverse() throws Exception {
        Map<String, String> options = new HashMap<String, String>();
        options.put("ingestURL",
                "https://dataverse.harvard.edu/api/search?key=c856dcf1-7df3-4bbc-82e8-311157d11281&q=*&type=dataset");
        options.put("documentElement", "items");
        options.put("cacheFilename", "dataverse_json");
        options.put("parserType", "json");
        options.put("offsetParam", "start");
        options.put("limitParam", "per_page");
        options.put("limitValue", "500");
        options.put("useCache", "false");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/dataverse_sample_record.json", 100);
    }

    public void testIngestLincsWeb() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://dev3.ccs.miami.edu:8080/dcic/api/fetchdata?searchTerm=*&limit=500");
        options.put("documentElement", "documents");
        options.put("cacheFilename", "lincs_json");
        options.put("parserType", "json");
        options.put("useCache", "false");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);

        ingestor.startup();
        boolean first = true;
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            if (first) {
                Utils.saveText(result.getPayload().toString(2), "/tmp/lincs_sample_doc.json");
                first = false;
            }
            System.out.println(result.getPayload());
            count++;
        }
        assertTrue(count > 0);
        System.out.println("# of JSON records:" + count);
    }

    public void testIngestWeb1() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "https://clinicaltrials.gov/search?term=&resultsxml=true");
        options.put("documentElement", "clinical_study");
        options.put("cacheFilename", "clinicaltrials_gov");
        options.put("filenamePattern", "\\w+\\.xml");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);

        ingestor.startup();
        boolean first = true;
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result);
            assertTrue(result.getStatus() != Result.Status.ERROR);
            if (first) {
                Utils.saveText(result.getPayload().toString(2), "/tmp/clinicaltrials_gov_record.json");
                first = false;
            }
            count++;
        }
        assertTrue(count > 0);
        System.out.println("# of xml records:" + count);
    }
}
