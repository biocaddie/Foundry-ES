package org.neuinfo.foundry.consumers.tool;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.neuinfo.foundry.consumers.tool.IngestionUtils.ingest;

/**
 * Created by bozyurt on 9/21/16.
 */
public class IngestionSampler {
    public void sampleViaTwoStageJSONIngestor() throws Exception {
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


    public void sampleIonChannelGen() throws Exception {
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

    public void sampleLincsCells() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/cells/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_cells_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, 5);
    }


    public void sampleLincsSM() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/sm/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_sm_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, -1);
        System.out.println("count=" + count);
    }

    public void sampleLincsDS() throws Exception {
        String ingestURL = "http://lincs.hms.harvard.edu/db/datasets/?search=&output_type=.csv";
        String outFile = "/tmp/lincs_ds_summary_csv_record.json";
        int count = ingestCSV(ingestURL, outFile, -1);
        System.out.println("count=" + count);
    }

    public void sampleGemma() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL",
                "http://www.chibi.ubc.ca/Gemma/datasetdownload/4.20.2011/DatasetSummary.view.txt.gz");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", "\t");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");

        NIFCSVIngestor ingestor = new NIFCSVIngestor();
        ingestor.initialize(options);
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/gemma_record.json", 5);
    }

    public void sampleSRA() throws Exception {
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

    public void sampleDbGaP() throws Exception {
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

    public void sampleBDSC() throws Exception {
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

    public void sampleBlueBrain_Bluima_Connectivity() throws Exception {
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


    public void sampleBMRB() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.bmrb.wisc.edu");
        options.put("remotePath", "/pub/bmrb/entry_lists/xml/");
        options.put("filenamePattern", ".+\\.xml\\.gz$");
        options.put("recursive", "true");
        options.put("outDir", "/var/data/foundry-es/cache/data/BMRB");
        // options.put("topElement", "datablock");
        options.put("documentElement", "datablock");
        options.put("sampleMode", "true");
        options.put("sampleSize", "10");

        FTPIngestor ingestor = new FTPIngestor();
        // ingestor.setTestMode(true);
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/bmrb_xml_record.json", 5);
    }

    public void samplePDBFromFTP() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.wwpdb.org");
        options.put("remotePath", "/pub/pdb/data/structures/divided/XML/");
        options.put("filenamePattern", ".+\\.xml\\.gz$");
        options.put("recursive", "true");
        //  options.put("outDir", "/tmp/pdb_ftp");
        options.put("maxDocs", "10");

        options.put("documentElement", "datablock");


        String transformScript = TransformMappingUtils.loadTransformMappingScriptFromClasspath("transform/pdb.trs");
        System.out.println(transformScript);
        options.put("transformScript", transformScript);

        FTPIngestor ingestor = new FTPIngestor();
        ingestor.setTestMode(true);
        ingestor.initialize(options);
        ingestor.startup();
        if (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            Utils.saveText(result.getPayload().toString(2), "/tmp/pdb_xml_record.json");
        }
    }

    public void sampleGEOByAspera() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("filenamePattern", "\\.soft\\.gz$");
        options.put("documentElement", "MINiML");
        options.put("source", "anonftp@ftp.ncbi.nlm.nih.gov:/geo/datasets/");
        options.put("dest", "/var/data/geo");
        options.put("publicKeyFile", "/home/bozyurt/.aspera/connect/etc/asperaweb_id_dsa.openssh");
        options.put("arguments", "-k1 -Tr -l200m");
        options.put("fullSet", "false");
        options.put("xmlFileNamePattern", "\\.xml$");
        options.put("parserType", "geo");

        AsperaIngestor ingestor = new AsperaIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/geo_datasets_soft_record.json", 5);
    }

    public void sampleBioproject() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.ncbi.nlm.nih.gov");
        options.put("outDir", "/var/data/foundry-es/cache/data");
        options.put("documentElement", "Package");
        options.put("topElement", "PackageSet");
        options.put("remotePath", "bioproject/bioproject.xml");

        FTPIngestor ingestor = new FTPIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/bioproject_sample.json", 5);
    }

    public void sampleBioModelsViaFTP() throws Exception {
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
        ingest(ingestor, "/tmp/biomodels_xml_record.json", 5);
    }

    public void sampleWiredScienceRSS() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.wired.com/category/science/feed/");
        RSSIngestor ingestor = new RSSIngestor();
        ingestor.initialize(options);
        ingestor.startup();
        ingest(ingestor, "/tmp/wired_news_record.json", 5);
    }

    public void sampleNeuromorpho() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00006_1", "/tmp/neuromorpho_record.json", 5);
    }

    public void sampleCellImageLibrary() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_37639_1", "/tmp/cell_image_library_record.json", 5);
    }

    public void sampleNeuroElectro() throws Exception {
        ingestSampleFromDisco("pr_nlx_151885_1", "/tmp/neuroelectro_record.json", 5);
    }

    public void sampleICG() throws Exception {
        ingestSampleFromDisco("pr_scr_014194_1", "/tmp/ion_channel_geneology_record.json", 5);
    }

    public void sampleAllenCellTypesMorpho() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00508_5", "/tmp/allen_cell_types_morphology_record.json", 5);
    }

    public void sampleAllenCellTypesElectroPhysio() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_00508_4", "/tmp/allen_cell_types_electrophysiology_record.json", 5);
    }

    public void sampeIntegratedConnectivity() throws Exception {
        ingestSampleFromDisco("pr_nlx_154697_8", "/tmp/integrated_connectivity_record.json", 5);
    }

    public void samplePeptideAtlasWithTransform() throws Exception {
        ingestSampleFromDiscoWithTransform("pr_nif_0000_03266_2", "/tmp/peptide_atlas_record.json",
                5, "dvp", "peptideatlas.trs");
    }

    public void sampleOpenFMRI() throws Exception {
        ingestSampleFromDisco("pr_nlx_144048_1", "/tmp/openfmri_record.json", 5);
    }

    public void sampleMPD() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_03160_1", "/tmp/mouse_phenome_db_record.json", 5);
    }

    public void samplePhysiobank() throws Exception {
        ingestSampleFromDisco("pr_nlx_48903_1", "/tmp/physiobank_record.json", 5);
    }

    public void sampleProteomeXchange() throws Exception {
        ingestSampleFromDisco("pr_nlx_158620_1", "/tmp/proteome_xchange_record.json", 5);
    }

    public void sampleYPED() throws Exception {
        ingestSampleFromDisco("pr_nlx_152660_1", "/tmp/yped_record.json", 5);
    }

    public void sampleCIA() throws Exception {
        ingestSampleFromDisco("pr_nlx_151749_1", "/tmp/cia_record.json", 5, "dv");
    }

    public void sampleNURSA() throws Exception {
        ingestSampleFromDisco("nif_0000_03208_2", "/tmp/nursa_datasets_record.json", 5, "dv");
    }

    public void sampleNIDDK() throws Exception {
        ingestSampleFromDisco("pr_nlx_152673_1", "/tmp/niddk_central_rep_record.json", 5);
    }

    public void sampleCTN() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_21981_1", "/tmp/clinical_trials_network_record.json", 5);
    }

    public void sampleNeuroelectroRaw() throws Exception {
        ingestSampleFromDiscoRaw("l2_nlx_151885_data_summary", null, "/tmp/neuroelectro_record.json", 5);
    }

    public void sampleIntegratedConnectivity() throws Exception {
        ingestSampleFromDisco("pr_nlx_154697_8", "/tmp/integrated_connectivity_record.json", 5);
    }

    public void ingestNITRC_IR() throws Exception {
        ingestSampleFromDisco("pr_nlx_18447_1", "/tmp/nitrc_ir_record.json", 5);
    }

    public void sampleRGD() throws Exception {
        String tableNames = "l2_nif_0000_00134_data_rattus_strains_mp a,l2_nlx_83784_data_term b, l2_nif_0000_00134_data_strain_detail c";
        String joinInfo = "a.go_id=b.id, a.db_object_id=c.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/RGD_record.json", 5);
    }


    public void sampleEUClinicalTrials() throws  Exception {
        String tableNames = "l2_nlx_151313_clinicaltrial_summary a, l2_nlx_151313_clinicaltrial_summary_disease b";
        String joinInfo = "a.eudract_number=b.eudract_number";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/eu_clinical_trials_record.json", 5);
    }

    public void sampleCellosaurus() throws Exception {
        String tableNames = "l2_scr_013869_cellosaurus_cellosaurus";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/cellosaurus_record.json", 5);
    }

    public void sampleBrainMaps() throws Exception {
        String tableNames = "l2_nif_0000_00093_brainmaps_brainimage a, l2_nif_0000_00093_brainmaps_dataset b";
        String joinInfo = "a.id = b.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/brainmaps_record.json", 5);
    }

    public void sampleCocomac() throws Exception {
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


    public void sampleConnectomeWiki() throws Exception {
        // nif-0000-24441
        String tableNames = "l2_nif_0000_24441_CONNECTOMEWIKI_NAME a," +
                "l2_nif_0000_24441_CONNECTOMEWIKI_FROM b, " +
                "l2_nif_0000_24441_CONNECTOMEWIKI_NODE c," +
                "l2_nif_0000_24441_CONNECTOMEWIKI_TO d";
        String joinInfo = "a.term = b.z, b.x = c.x, b.x = d.x";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/connectome_wiki_record.json", 100);
    }

    public void sampleFlybase() throws Exception {
        // original data from postgres db
        String tableNames = "l2_nif_0000_00558_fb_chado_stock a," +
                "l2_nif_0000_00558_fb_chado_organism b," +
                "l2_nif_0000_00558_fb_chado_stock_genotype c," +
                "l2_nif_0000_00558_fb_chado_genotype d";
        String joinInfo = "a.organism_id=b.organism_id, " +
                "a.stock_id=c.stock_id, c.genotype_id=d.genotype_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/flybase_record.json", 5);
    }

    public void sampleBCBC() throws Exception {
        String tableNames = "l2_nlx_144143_adenovirus_list a," +
                "l2_nlx_144143_adenovirus_information b," +
                "l2_nlx_144143_adenovirus_publication c";
        String joinInfo = "a.id = b.id, a.id = c.id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/bcbc_record.json", 100);
    }

    public void sampleTemporalLobe() throws Exception {
        String tableNames = "l2_nif_0000_24805_data_list";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/temporal_lobe_record.json", 5);
    }

    public void sampleNXR() throws Exception {
        String tableNames = "l2_scr_013731_xenopus_laevis";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/nxr_record.json", 5);
    }

    public void sampleIMSR() throws Exception {
        String tableNames = "l2_nif_0000_09876_data_strain";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/imsr_record.json", 5);
    }

    public void sampleZFIN() throws Exception {
        String tableNames = "l2_nif_0000_21427_data_genotypes a, " +
                "l2_nif_0000_21427_data_genotype_background b," +
                "l2_nif_0000_21427_data_wildtype_genotypes w";
        String joinInfo = "a.zfin_genotype_id = b.genotype_id, b.background_genotype_id = w.zfin_genotype_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/zfin_record.json", 100);
    }

    public void sampleDGGR() throws Exception {
        String tableNames = "l2_nif_0000_30415_data_detail";
        ingestSampleFromDiscoRaw(tableNames, null, "/tmp/dggr_record.json", 5);
    }

    public void sampleAntibodyRegistry() throws Exception {
        String tableNames = "l2_nif_0000_07730_nif_eelg_antibody_table a, " +
                "l2_nif_0000_07730_nif_eelg_antibody_vendor d," +
                "l2_nif_0000_07730_antibody_pmid p";
        String joinInfo = "a.vendor_id=d.id, a.id = p.ab_id";
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/ar_record.json", 5);
    }

    public void sampleWormbase() throws Exception {
        String tableNames = "l2_nif_0000_00053_strain_rrid_strain_data a";
        String joinInfo = null;
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/wormbase_record.json", 5);
    }

    public void sampleZIRC() throws Exception {
        String tableNames = "l2_nif_0000_00242_catalog_line";
        String joinInfo = null;
        ingestSampleFromDiscoRaw(tableNames, joinInfo, "/tmp/zirc_record.json", 5);
    }

    public void sampleTSC() throws Exception {
        String ingestURL = "https://tetrahymena.vet.cornell.edu/extras/TetrahymenaRRID.csv";
        String outFile = "/tmp/tsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void sampleXGSC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/RRID_codes_Xiphophorus.csv";
        String outFile = "/tmp/xgsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void sampleAGSC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/AmbystomaCatalog.csv";
        String outFile = "/tmp/agsc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void sampleNSRRC() throws Exception {
        String ingestURL = "file:///home/bozyurt/dev/java/SciCrunch-Curation/StaticData/NSRRC_RRID.csv";
        String outFile = "/tmp/nsrrc_record.json";
        ingestCSV(ingestURL, outFile, 5);
    }

    public void sampleAntibodies() throws Exception {
        ingestSampleFromDisco("pr_nif_0000_07730_1", "/tmp/ar_antibodies_record.json", 5);
    }


    public void sampleDryad() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.datadryad.org/oai/request");
        options.put("sourceName", "Dryad");
        options.put("topElement", "ListRecords");
        options.put("documentElement", "record");
        // options.put("testMode", "true");
        OAIIngestor ingestor = new OAIIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/dryad_oai_record.json", 5);
    }

    public void sampleArXiv() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://export.arxiv.org/oai2");
        options.put("sourceName", "ArXiv");
        options.put("topElement", "ListRecords");
        options.put("documentElement", "record");
        options.put("metadataPrefix", "arXivRaw");
        OAIIngestor ingestor = new OAIIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/arxiv_oai_record.json", 5);
    }

    public void sampleCVRG() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        //options.put("ingestURL", "https://eddi.cvrgrid.org/oai/request?verb=ListMetadataFormats");
        options.put("ingestURL", "https://eddi.cvrgrid.org/oai/request");
        // options.put("ingestURL", "http://cvrgrid.org/oai/request");
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

    public void sampleICPSR() throws Exception {
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

    public void sampleOSB() throws Exception {
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
        ingest(ingestor, "/tmp/osb_record.json", 5);
    }

    public void sampleArrayExpress() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://www.ebi.ac.uk/arrayexpress/xml/v2/experiments");
        options.put("topElement", "experiments");
        options.put("documentElement", "experiment");

        NIFXMLIngestor ingestor = new NIFXMLIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/ae/array_express_record.json", 5000);
    }

    public void sampleDataverse() throws Exception {
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

    public void sampleLincsWeb() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://lincsportal.ccs.miami.edu/dcic/api/fetchdata?searchTerm=*&limit=500");
        options.put("documentElement", "documents");
        options.put("cacheFilename", "lincs_json");
        options.put("parserType", "json");
        options.put("useCache", "false");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/lincs_sample_doc.json", 5);
    }

    public void sampleClinicalTrials() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "https://clinicaltrials.gov/search?term=&resultsxml=true");
        options.put("documentElement", "clinical_study");
        options.put("cacheFilename", "clinicaltrials_gov");
        options.put("filenamePattern", "\\w+\\.xml");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/clinicaltrials_gov_record.json", 5);
    }

    public void sampleEBI() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///home/bozyurt/Downloads/from_nansu/ebi/all_data_ebi.json");
        options.put("documentElement", "data");
        options.put("cacheFilename", "ebi");
        options.put("useCache", "false");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/ebi_record.json", 5);
    }

    public void sampleGenenetwork() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///home/bozyurt/Downloads/from_nansu/genenetwork/all_data_genenetwork.json");
        options.put("documentElement", "data");
        options.put("cacheFilename", "genenetwork");
        options.put("useCache", "false");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/genenetwork_record.json", 5);
    }

    public void sampleLSDBFromNansu() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///home/bozyurt/Downloads/from_nansu/LSDB/all_data_LSDB.json");
        options.put("documentElement", "data");
        options.put("cacheFilename", "lsdb");
        options.put("useCache", "false");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/lsdb_record.json", 5);
    }

    public void sampleHmps() throws Exception {
        sampleHmp("hmp_NCBI_reference_genomics", "all_data_hmp_NCBI_reference_genomics.json");
        sampleHmp("hmp_NCBI_metagenomic_shotgun_sequence", "all_data_hmp_NCBI_shotgun_sequence.json");
        sampleHmp("hmp_NCBI_metagenomic_16s_sequence", "all_data_hmp_NCBI_Metagenomic_16S_Sequence.json");
        sampleHmp("hmp_catalog_reference_genomics", "all_data_hmp_catalog_reference_genomics.json");
        sampleHmp("hmp_catalog_metagenomic_samples", "all_data_hmp_catalog_metagenomic_samples.json");
    }

    public void sampleHmp(String name, String dataFile) throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///home/bozyurt/Downloads/from_nansu/hmp/" + dataFile);
        options.put("documentElement", "data");
        options.put("cacheFilename", name);
        options.put("useCache", "false");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        new File("/tmp/hmp").mkdir();
        ingest(ingestor, "/tmp/hmp/" + name + "_record.json", 5);
    }

    public void sampleNeuroVaultAtlases() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://neurovault.org/api/atlases/");
        options.put("documentElement", "results");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/neurovault_atlases_record.json", 5);
    }

    public void sampleNeuroVaultNIDM() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://neurovault.org/api/nidm_results/");
        options.put("documentElement", "results");
        options.put("parserType", "json");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/neurovault_nidm_results_record.json", 5);
    }

    public void sampleNeuroVaultCollections() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "http://neurovault.org/api/collections/");
        options.put("documentElement", "results");
        options.put("parserType", "json");
        options.put("limitParam", "limit");
        options.put("limitValue", "100");
        options.put("offsetParam", "offset");
        options.put("useCache", "false");
        options.put("mergeIngestURLTemplate", "http://neurovault.org/api/collections/${id}/images/");
        options.put("idJsonPath", "$.id");
        options.put("mergeFieldName", "images");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/neurovault_collections_record.json", 50);
    }

    public void sampleSwissProt() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz");
        options.put("topElement", "uniprot");
        options.put("documentElement", "entry");
        options.put("cacheFilename", "uniprot_sprot.xml");
        options.put("useCache", "true");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/swissprot_record.json", 5);
    }

    public void sampleUniprotTrEMBL() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.xml.gz");
        options.put("topElement", "uniprot");
        options.put("documentElement", "entry");
        options.put("cacheFilename", "uniprot_trembl.xml");
        options.put("useCache", "true");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/uniprot_tremble_record.json", 5);
    }

    public void sampleFromPubMed() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline");
        options.put("topElement", "MedlineCitationSet");
        options.put("documentElement", "MedlineCitation");
        options.put("filenamePattern", "\\w+\\.xml\\.gz");
        PubMedIngestor ingestor = new PubMedIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/pubmed_records.json", 10000);
    }


    public void sampleLSDB() throws Exception {
        Map<String, String> options = new HashMap<String, String>();
        options.put("ingestURL",
                "file:///var/data/foundry-es/cache/data/LSDB/datameta_en.json");
        options.put("documentElement", "data");
        options.put("cacheFilename", "datameta_en.json");
        options.put("parserType", "json");
        options.put("useCache", "true");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/lsdb_sample_record.json", 5);
    }


    public static void extractJSONLDRecord() throws IOException {
        String HOME_DIR = System.getProperty("user.home");
        String jsonStr = Utils.loadAsString(HOME_DIR + "/Downloads/ImmPort.JSON-LD.example.Investigation_18.json");
        JSONObject json = new JSONObject(jsonStr);
        JSONArray studies = json.getJSONArray("study");
        // System.out.println(studies.getJSONObject(0).toString(2));
        String outFile = "/tmp/jsonld_study_record.json";
        Utils.saveText(studies.getJSONObject(0).toString(2), outFile);
        System.out.println("saved file:" + outFile);
    }


    public void sampleFromPubmedIncrementalData() throws Exception {
        String[] refTypes = {"AssociatedDataset", "AssociatedPublication", "CommentOn", "CommentIn", "ErratumIn",
                "ErratumFor", "PartialRetractionIn", "PartialRetractionOf", "RepublishedFrom", "RepublishedIn",
                "RetractionOf", "RetractionIn", "UpdateIn", "UpdateOf", "SummaryForPatientsIn", "OriginalReportIn",
                "ReprintOf", "ReprintIn", "Cites"};

        String[] mainElements = {"Article", "ArticleDate", "ArticleTitle", "Author", "AuthorList", "Chemical",
                "ChemicalList", "CitationSubset", "CollectiveName", "CommentsCorrections", "CommentsCorrectionsList",
                "CopyrightInformation", "Country", "DataBank", "DataBankList", "DataBankName", "DateCompleted",
                "DateCreated", "DateRevised", "Day", "DescriptorName", "ELocationID", "EndPage", "ForeName",
                "GeneSymbol", "GeneSymbolList", "GeneralNote", "Grant", "GrantList", "Identifier", "ISOAbbreviation",
                "ISSN", "ISSNLinking", "Initials", "Investigator", "InvestigatorList", "Issue", "Journal",
                "JournalIssue", "Keyword", "KeywordList", "Language", "MeshHeading", "MeshHeadingList", "OtherAbstract",
                "OtherID", "PMID", "Pagination", "PersonalNameSubject", "PersonalNameSubjectList", "PubDate", "PublicationType",
                "PublicationTypeList", "QualifierName", "SupplMeshList", "SupplMeshName", "Title", "VernacularTitle"
        };

        Set<String> tagSet = new HashSet<String>(Arrays.asList(mainElements));
        for (String refType : refTypes) {
            tagSet.add(refType);
        }
        String homeDir = System.getProperty("user.home");
        List<File> localFiles = new ArrayList<File>(1);
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016a_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016b_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016c_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016d_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016e_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016f_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medsamp2016h_formatted.xml"));
        localFiles.add(new File(homeDir + "/Downloads/medline16n0813_formatted.xml"));
        RemoteFileIterator rfi = new RemoteFileIterator(localFiles);
        XMLFileIterator xmlFileIterator = new XMLFileIterator(rfi, "MedlineCitationSet", "MedlineCitation");
        int sampleSize = 5;
        int count = 0;
        String outFile = "/tmp/pubmed_sample_record.json";
        Set<String> seenSet = new HashSet<String>();
        while (xmlFileIterator.hasNext()) {
            String jsonFile = outFile.replaceFirst("\\.json$", "_" + (count + 1) + ".json");
            Element element = xmlFileIterator.next();

            try {
                Result r = ConsumerUtils.convert2JSON(element);
                String jsonStr = r.getPayload().toString(2);
                boolean ok = false;
                for (String s : tagSet) {
                    if (!seenSet.contains(s)) {
                        if (jsonStr.indexOf("\"" + s + "\"") != -1) {
                            ok = true;
                            seenSet.add(s);
                        }
                    }

                }
                if (ok) {
                    Utils.saveText(jsonStr, jsonFile);
                    System.out.println("saved file:" + jsonFile);
                    count++;
                }
                if (seenSet.size() == tagSet.size()) {
                    System.out.println("finished");
                    break;
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        System.out.println("==============");
        for (String s : tagSet) {
            if (!seenSet.contains(s)) {
                System.out.println(s);
            }
        }

    }

    public void sampleClinVarFromFTP() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.ncbi.nlm.nih.gov");
        options.put("remotePath", "/pub/clinvar/xml/ClinVarFullRelease_00-latest.xml.gz");
        options.put("outDir", "/var/data/foundry-es/cache/data");
        options.put("documentElement", "ClinVarSet");
        options.put("topElement", "ReleaseSet");

        FTPIngestor ingestor = new FTPIngestor();
        ingestor.initialize(options);
        ingestor.startup();

        int count = 0;

        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            String suffix = count == 0 ? "" : String.valueOf(count);
            Utils.saveText(result.getPayload().toString(2),
                    "/tmp/clinvar_sample" + suffix + ".json");
            count++;
            if (count == 10) {
                break;
            }
        }
    }

    public void ingestDataCiteBGI() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=bl.bgi", "/tmp/bgi_sample_record.json");
    }

    public void ingestDataCiteMendeley() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=bl.mendeley", "/tmp/mendeley_sample_record.json");
    }

    public void ingestDataCiteGNODE() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=TIB.G-NODE", "/tmp/gnode_sample_record.json", false);
    }

    public void ingestDataCiteMorphoBank() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.MORPHOBA", "/tmp/morphobank_sample_record.json");
    }

    public void ingestDataCiteSimTK() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.SIMTK", "/tmp/simtk_sample_record.json");
    }

    public void ingestDataCiteThieme() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=TIB.THIEME", "/tmp/thieme_sample_record.json");
    }

    public void ingestDataCiteGMS() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=ZBMED.GMS", "/tmp/gms_sample_record.json");
    }

    public void ingestDataCiteZenodo() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CERN.ZENODO", "/tmp/zenodo_sample_record.json");
    }

    public void ingestDataCitePeerj() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.PEERJ", "/tmp/peerj_sample_record.json");
    }

    public void ingestDataCiteLSHTM() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=BL.LSHTM", "/tmp/lshtm_sample_record.json");
    }

    public void ingestDataCiteDatabrary() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.DATABRAR", "/tmp/databrary_sample_record.json");
    }

    public void ingestDataCiteImmport() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.IMMPORT", "/tmp/immport_sample_record.json");
    }

    public void ingestDataCiteSDSCSG() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.SDSCSG", "/tmp/sdscsg_sample_record.json");
    }

    public void ingestDataCiteUCBCRCNS() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.UCBCRCNS", "/tmp/ucbcrcns_sample_record.json");
    }

    public void ingestDataCiteMIMH() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=OSTI.NIMH", "/tmp/mimh_sample_record.json");
    }

    public void ingestDataCiteCANDI() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.UMMSCAND", "/tmp/candi_sample_record.json");
    }

    public void ingestDataCiteADA() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=ANDS.CENTRE87", "/tmp/ada_sample_record.json");
    }

    public void ingestDataCiteCCDC() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=BL.CCDC", "/tmp/ccdc_sample_record.json");
    }

    public void ingestDataCiteMBFBioscience() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.MBFBIO", "/tmp/mbf_sample_record.json");
    }

    public void ingestDataCiteFDZ() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=GESIS.DEAS", "/tmp/fdz_sample_record.json");
    }

    public void ingestDataCiteUKDA() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=BL.UKDA", "/tmp/ukda_sample_record.json");
    }

    public void ingestDataCiteAdaptive() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.ADAPTIVE", "/tmp/adaptive_sample_record.json");
    }

    public void ingestDataCiteBroad() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.BROAD", "/tmp/broad_sample_record.json");
    }

    public void ingestDataCiteJHU() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.JHU", "/tmp/jhu_sample_record.json");
    }

    public void ingestDataCiteMITLCP() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.MITLCP", "/tmp/mitlcp_sample_record.json");
    }

    public void ingestDataCiteCTSI() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.UCSFCTSI", "/tmp/ctsi_sample_record.json");
    }

    public void ingestDataCiteARS() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=FIGSHARE.ARS", "/tmp/ars_sample_record.json");
    }

    public void ingestDataCiteCXIDB() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=OSTI.CXIDB", "/tmp/cxidb_sample_record.json");
    }

    public void ingestDataCiteBILS() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=SND.BILS", "/tmp/bils_sample_record.json");
    }

    public void ingestDataCiteSBGrid() throws Exception {
        ingestDataCite("http://api.datacite.org/dats?publisher-id=CDL.SBGRID", "/tmp/sbgrid_sample_record.json");
    }

    void ingestDataCite(String ingestURL, String outFile) throws Exception {
        ingestDataCite(ingestURL, outFile, true);
    }

    public void sampleOmics() throws Exception {
        Map<String, String> options = new HashMap<String, String>();
        options.put("ingestURL", "http://www.omicsdi.org/ws/dataset/search?query=");
        options.put("parserType", "json");
        options.put("documentElement", "datasets");
        options.put("offsetParam", "start");
        options.put("limitParam", "size");
        options.put("limitValue", "100");
        options.put("useCache", "true");
        options.put("sampleMode", "true");
        options.put("sampleSize", "1000");
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/omicsdi_sample_record.json", 200);
    }


    void ingestDataCite(String ingestURL, String outFile, boolean useFilter) throws Exception {
        Map<String, String> options = new HashMap<String, String>();
        options.put("ingestURL", ingestURL);
        options.put("parserType", "json");
        options.put("documentElement", "data");
        options.put("offsetParam", "offset");
        options.put("limitParam", "rows");
        options.put("limitValue", "100");
        options.put("useCache", "false");
        options.put("sampleMode", "true");
        options.put("sampleSize", "1000");
        if (useFilter) {
            options.put("filterJsonPath", "$.attributes.types[0].information.value.id");
            options.put("filterValue", "dataset");
        }
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, outFile, 10);
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

    public void processPayloadWithTransformation(Ingestor ingestor, String outFile, TransformationEngine trEngine) throws IOException {
        Result result = ingestor.prepPayload();
        JSONObject transformedJson = new JSONObject();
        trEngine.transform(result.getPayload(), transformedJson);
        System.out.println(transformedJson.toString(2));

        Utils.saveText(transformedJson.toString(2), outFile);
        System.out.println("saved file:" + outFile);
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
    
    public void sampleNSRRWeb() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///Users/rliu1/Desktop/datasets.json");
        options.put("documentElement", "");
        options.put("cacheFilename", "datameta_en.json");
        options.put("parserType", "json");
        options.put("useCache", "true");
        
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/nsrr_sample_doc.json", 5);
    }
    
    public void sampleGDC() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "file:///Users/rliu1/Desktop/GDC_projects.json");
        options.put("documentElement", "");
        options.put("cacheFilename", "datameta_en.json");
        options.put("parserType", "json");
        options.put("useCache", "true");
        
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        ingest(ingestor, "/tmp/gdc_sample_doc.json", 5);
    }

    public static void main(String[] args) throws Exception {
        IngestionSampler sampler = new IngestionSampler();

        // sampler.sampleCVRG();
        // sampler.sampleBMRB();
        // sampler.sampleSwissProt();
        // sampler.sampleUniprotTrEMBL();

        //  sampler.sampleFromPubMed();

        //sampler.sampleFromPubmedIncrementalData();

        // sampler.sampleArXiv();

        // sampler.sampleLSDB();;

        // sampler.sampleNeuroVaultAtlases();
        // sampler.sampleNeuroVaultNIDM();
        //sampler.sampleNeuroVaultCollections();

        // extractJSONLDRecord();
        // sampler.sampleClinVarFromFTP();
        // sampler.sampleLincsWeb();
        //  sampler.sampleOpenFMRI();
        //sampler.sampleGEOByAspera();
        //sampler.ingestNITRC_IR();

        // sampler.ingestDataCiteBGI();
        //sampler.ingestDataCiteMendeley();
        // sampler.ingestDataCiteGNODE();
        // sampler.ingestDataCiteMorphoBank();
        //    sampler.ingestDataCiteSimTK();

        // sampler.ingestDataCiteThieme();
        //    sampler.ingestDataCiteGMS(); // no filter
        // sampler.ingestDataCiteZenodo();
        // sampler.ingestDataCitePeerj();

        // sampler.ingestDataCiteLSHTM();
        //  sampler.ingestDataCiteDatabrary();
        // sampler.ingestDataCiteImmport();
        // sampler.ingestDataCiteSDSCSG();
        // sampler.ingestDataCiteUCBCRCNS();

        // sampler.ingestDataCiteMIMH();
        // sampler.ingestDataCiteCANDI();

        // sampler.ingestDataCiteADA();
        // sampler.ingestDataCiteCCDC();
        //  sampler.ingestDataCiteMBFBioscience();
        //  sampler.ingestDataCiteFDZ();
        // sampler.ingestDataCiteGMS(); // no data sets

        //     sampler.ingestDataCiteUKDA();
//        sampler.ingestDataCiteAdaptive();
//        sampler.ingestDataCiteBroad(); // no data sets
        //       sampler.ingestDataCiteJHU();
        //      sampler.ingestDataCiteMITLCP();
        //        sampler.ingestDataCiteCTSI();
        //  sampler.ingestDataCiteARS();
        //  sampler.ingestDataCiteCXIDB();
//        sampler.ingestDataCiteBILS();
        // sampler.ingestDataCiteSBGrid();

        // sampler.sampleOmics();
          sampler.sampleClinicalTrials();

        // sampler.sampleEBI();
        // sampler.sampleGenenetwork();
        // sampler.sampleLSDBFromNansu();

       // sampler.sampleHmps();
       // sampler.sampleEUClinicalTrials();
    }
}
