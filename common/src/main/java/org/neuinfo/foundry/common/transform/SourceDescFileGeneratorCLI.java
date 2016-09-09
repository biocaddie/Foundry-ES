package org.neuinfo.foundry.common.transform;

import org.apache.commons.cli.*;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.IngestMethod;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.SourceInfo;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by bozyurt on 9/17/15.
 */
public class SourceDescFileGeneratorCLI {
    static final String HOME_DIR = System.getProperty("user.home");

    public static void generateFlystock() throws Exception {
        SourceInfo si = new SourceInfo("nif-0000-00241",
                "Bloomington Drosophila Stock Center", "nif-0000-00241", IngestMethod.CSV);
        si.setIngestURL("http://flystocks.bio.indiana.edu/bloomington.csv");
        si.setPrimaryKeyJsonPath("$.'Stk #'");
        si.setTransformScript("transform/flystocks.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/nif-0000-00241.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }


    public static void generateLincsCells() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0002",
                "LINCS Cells", "biocaddie-0002", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/cells/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'Cell Name'");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/lincs_cells.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0002.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateLincsSM() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0003",
                "LINCS Small Molecules", "biocaddie-0003", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/sm/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'Small Mol Name'");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/lincs_sm.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0003.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateLincsDSSummary() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0004",
                "LINCS Datasets Summary", "biocaddie-0004", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/datasets/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'HMS Dataset ID'");
        si.setTransformScript(HOME_DIR +
                "/dev/java/Foundry-Data/transformations/lincs_ds_summary_2.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File(HOME_DIR +
                "/dev/java/Foundry-ES/consumers/etc/biocaddie-0004.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateLincsDSResults() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0005",
                "LINCS Datasets Results", "biocaddie-0005", IngestMethod.RESOURCE);
        si.setRepositoryID("nlx_156062");
        si.addPrimaryKeyJsonPath("hmsDatasetID")
                .addPrimaryKeyJsonPath("Small Molecule HMS LINCS Batch ID")
                .addPrimaryKeyJsonPath("Cell Name")
                .addPrimaryKeyJsonPath("Protein HMS LINCS ID");

        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/lincs_ds_result.trs");

        si.setMappingScript(HOME_DIR + "/dev/java/Foundry-Data/mappings/lincs_ds_result.ms");

        si.setContentSpecParam("sourceURL", "ds:biocaddie-0004::HMS Dataset ID");
        si.setContentSpecParam("urlTemplate", "http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv");
        si.setContentSpecParam("fileType", "csv");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");
        si.setContentSpecParam("fieldsToAdd", "HMS Dataset ID:hmsDatasetID");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0005.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generatePDBSource() throws Exception {
        SourceInfo si = new SourceInfo("nif-0000-00135",
                "RCSB Protein Data Bank", "nif-0000-00135", IngestMethod.RSYNC);

        // primary key for PDB
        si.setPrimaryKeyJsonPath("$.'PDBx:datablock'.'@datablockName'");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/pdb.trs");
        si.setContentSpecParam("rsyncSource", "rsync.wwpdb.org::ftp_data/structures/divided/XML-noatom/");
        si.setContentSpecParam("rsyncDest", "/var/temp/pdb_rsync");
        si.setContentSpecParam("filenamePattern", ".+\\.xml\\.gz$");
        si.setContentSpecParam("largeRecords", "true");
        si.setContentSpecParam("port", "33444");
        si.setContentSpecParam("documentElement", "datablock");
        si.setContentSpecParam("fullSet", "false");
        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/pdb_rsync_gen.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateGEODatasetSource() throws IOException {
        SourceInfo si = new SourceInfo("biocaddie-0006",
                "GEO Datasets", "biocaddie-0006", IngestMethod.ASPERA);
        si.setRepositoryID("nif-0000-00142");
        si.setPrimaryKeyJsonPath("$.'subset_dataset_id'");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/geo_dataset.trs");
        si.setContentSpecParam("filenamePattern", "\\d\\.soft\\.gz$");
        si.setContentSpecParam("documentElement", "MINiML");
        si.setContentSpecParam("source", "anonftp@ftp.ncbi.nlm.nih.gov:/geo/datasets/ ");
        si.setContentSpecParam("dest", "/var/data/geo");
        si.setContentSpecParam("publicKeyFile", "/var/home/bozyurt/.aspera/connect/etc/asperaweb_id_dsa.openssh");
        si.setContentSpecParam("arguments", "-k1 -Tr -l200m");
        si.setContentSpecParam("fullSet", "true");
        si.setContentSpecParam("xmlFileNamePattern", "\\.xml$");
        si.setContentSpecParam("parserType", "geo");
        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0006.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateGemma() throws Exception {

        SourceInfo si = new SourceInfo("biocaddie-0007",
                "Gemma", "biocaddie-0007", IngestMethod.CSV);
        si.setRepositoryID("nif-0000-08127");
        // http://www.chibi.ubc.ca/Gemma/datasetdownload/4.20.2011/DatasetSummary.view.txt.gz
        si.setIngestURL("http://www.chibi.ubc.ca/Gemma/datasetdownload/4.20.2011/DatasetSummary.view.txt.gz");
        si.setPrimaryKeyJsonPath("$.GemmaDsId");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/gemma.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", "\t");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0007.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateBioproject() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0008",
                "Bioproject", "biocaddie-0008", IngestMethod.FTP);
        si.setRepositoryID("nlx_143909");
        si.setPrimaryKeyJsonPath("$.'Package'.'Project'.'Project'.'ProjectID'.'ArchiveID'.'@accession'");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/bioproject.trs");
        si.setContentSpecParam("ftpHost", "ftp.ncbi.nlm.nih.gov");
        si.setContentSpecParam("outDir", "/var/data/foundry-es/cache/data");
        si.setContentSpecParam("documentElement", "Package");
        si.setContentSpecParam("topElement", "PackageSet");
        si.setContentSpecParam("remotePath", "bioproject/bioproject.xml");


        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0008.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateArrayExpress() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0009",
                "ArrayExpress", "biocaddie-0009", IngestMethod.XML);
        si.setRepositoryID("nif-0000-30123");
        si.setIngestURL("http://www.ebi.ac.uk/arrayexpress/xml/v2/experiments");
        si.setPrimaryKeyJsonPath("$.experiment.id");
        si.setTransformScript(HOME_DIR + "/dev/java/Foundry-Data/transformations/arrayexpress.trs");
        si.setContentSpecParam("topElement", "experiments");
        si.setContentSpecParam("documentElement", "experiment");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0009.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    public static void generateNeuromorpho() throws Exception {
        Properties props = Utils.loadProperties("disco.properties");
        SourceInfo si = new SourceInfo("biocaddie-0010",
                "neuromorpho", "biocaddie-0010", IngestMethod.DISCO);
        si.setRepositoryID("nif-0000-00006");
        si.setPrimaryKeyJsonPath("$.id"); // FIXME
        si.setContentSpecParam("jdbcURL", props.getProperty("dbURL"));
        si.setContentSpecParam("dbUser", props.getProperty("user"));
        si.setContentSpecParam("dbPassword", props.getProperty("password"));
        si.setContentSpecParam("tableName", "pr_nif_0000_00006_1");
        si.setContentSpecParam("schemaName", "dvp");

        File outFile = new File(HOME_DIR + "/dev/java/Foundry-ES/consumers/etc/biocaddie-0010.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
    }

    static boolean handle(String configFile, String source) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Map<String, String>> descConfigs = (Map<String, Map<String, String>>)
                yaml.load(new FileInputStream(configFile));
        Map<String, String> descConfig = descConfigs.get(source);
        if (descConfig == null) {
            return false;
        }

        Set<String> excludeSet = new HashSet<String>(Arrays.asList("sourceID", "name", "ingestMethod",
                "repositoryID", "transformScript", "primaryKeyJSONPath", "ingestURL"));
        //System.out.println(descConfig);
        String sourceID = descConfig.get("sourceID");
        String name = descConfig.get("name");
        String ingestMethodStr = descConfig.get("ingestMethod");
        String repositoryID = descConfig.get("repositoryID");
        String primaryKeyJSONPath = descConfig.get("primaryKeyJSONPath");
        String ingestURL = descConfig.get("ingestURL");
        String transformScript = descConfig.get("transformScript");
        IngestMethod ingestMethod = IngestMethod.valueOf(ingestMethodStr.toUpperCase());
        Assertion.assertTrue(ingestMethod != null, "Not a supported ingest method:" + ingestMethodStr);
        SourceInfo si = new SourceInfo(sourceID, name, sourceID, ingestMethod);
        si.setTransformScript(transformScript);
        si.setPrimaryKeyJsonPath(primaryKeyJSONPath);
        if (ingestURL != null) {
            si.setIngestURL(ingestURL);
        }
        si.setRepositoryID(repositoryID);
        for (String key : descConfig.keySet()) {
            if (!excludeSet.contains(key)) {
                String value = descConfig.get(key);
                si.setContentSpecParam(key, value);
            }
        }
        File outFile = new File("/tmp/" + sourceID + ".json");
        SourceDescFileGenerator.prepareSource(si, outFile);
        System.out.println("wrote " + outFile);
        return true;
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SourceDescFileGeneratorCLI", options);
        System.exit(1);
    }


    public static void main(String[] args) throws Exception {
        //"one of [pdb, sm, cells, ds_summary, ds, geo, gemma, bioproject, arrayexpress, neuromorpho]"
        Option help = new Option("h", "print this message");
        Option sourceOption = Option.builder("s").argName("source").hasArg()
                .desc("source name (top level element) in the source-descriptor-cfg-file [e.g. pdb, dryad]").build();
        Option configFileOption = Option.builder("c").argName("source-descriptor-cfg-file").hasArg()
                .desc("Full path to the source descriptor config params YAML file").build();
        sourceOption.setRequired(true);
        configFileOption.setRequired(true);

        Options options = new Options();
        options.addOption(help);
        options.addOption(sourceOption);
        options.addOption(configFileOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line.hasOption("h")) {
            usage(options);
        }
        String configFile = line.getOptionValue('c');
        String source = line.getOptionValue('s');

        handle(configFile, source);
        /*
        if (source.equals("pdb")) {
            generatePDBSource();
        } else if (source.equals("sm")) {
            generateLincsSM();
        } else if (source.equals("cells")) {
            generateLincsCells();
        } else if (source.equals("ds_summary")) {
            generateLincsDSSummary();
        } else if (source.equals("ds")) {
            generateLincsDSResults();
        } else if (source.equals("geo")) {
            generateGEODatasetSource();
        } else if (source.equals("gemma")) {
            generateGemma();
        } else if (source.equals("bioproject")) {
            generateBioproject();
        } else if (source.equals("arrayexpress")) {
            generateArrayExpress();
        } else if (source.equals("neuromorpho")) {
            generateNeuromorpho();
        }
        */
    }
}
