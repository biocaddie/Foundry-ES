package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.SourceInfo;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.IngestMethod;

import java.io.File;


/**
 * Created by bozyurt on 5/5/15.
 */
public class SourceDescFileGeneratorTest extends TestCase {
    public SourceDescFileGeneratorTest(String name) {
        super(name);
    }

    public void testFlystockGeneration() throws Exception {
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

        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/nif-0000-00241.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }

    public void testLincsCellsGeneration() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0002",
                "LINCS Cells", "biocaddie-0002", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/cells/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'Cell Name'");
        si.setTransformScript("/var/burak/dev/java/Foundry-Data/transformations/lincs_cells.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/biocaddie-0002.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }

     public void testLincsSMGeneration() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0003",
                "LINCS Small Molecules", "biocaddie-0003", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/sm/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'Small Mol Name'");
        si.setTransformScript("/var/burak/dev/java/Foundry-Data/transformations/lincs_sm.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/biocaddie-0003.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }


    public void testLincsDSSummaryGeneration() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0004",
                "LINCS Datasets Summary", "biocaddie-0004", IngestMethod.CSV);
        si.setIngestURL("http://lincs.hms.harvard.edu/db/datasets/?search=&output_type=.csv");
        si.setPrimaryKeyJsonPath("$.'HMS Dataset ID'");
        si.setTransformScript("/var/burak/dev/java/Foundry-Data/transformations/lincs_ds_summary.trs");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");

        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/biocaddie-0004.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }


    public void testLincsDSResultsGeneration() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-0005",
                "LINCS Datasets Results", "biocaddie-0005", IngestMethod.RESOURCE);
        si.addPrimaryKeyJsonPath("hmsDatasetID")
                .addPrimaryKeyJsonPath("Small Molecule HMS LINCS Batch ID")
                .addPrimaryKeyJsonPath("Cell Name")
                .addPrimaryKeyJsonPath("Protein HMS LINCS ID");

        si.setTransformScript("/var/burak/dev/java/Foundry-Data/transformations/lincs_ds_result.trs");

        si.setMappingScript("/var/burak/dev/java/Foundry-Data/mappings/lincs_ds_result.ms");

        si.setContentSpecParam("sourceURL","ds:biocaddie-0004::HMS Dataset ID");
        si.setContentSpecParam("urlTemplate","http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv");
        si.setContentSpecParam("dataType","csv");
        si.setContentSpecParam("keepMissing", "false");
        si.setContentSpecParam("ignoreLines", "1");
        si.setContentSpecParam("headerLine", "1");
        si.setContentSpecParam("delimiter", ",");
        si.setContentSpecParam("textQuote", "&#034;");
        si.setContentSpecParam("escapeCharacter", "&#092;");
        si.setContentSpecParam("locale", "en_US");
        si.setContentSpecParam("fieldsToAdd", "HMS Dataset ID:hmsDatasetID");

        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/biocaddie-0005.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }

    public void testPDBSourceGeneration() throws Exception {
        SourceInfo si = new SourceInfo("biocaddie-pdb-0001",
                "RCSB Protein Data Bank", "biocaddie-pdb-0001", IngestMethod.FTP);

        // primary key for PDB
        si.setPrimaryKeyJsonPath("$.'PDBx:datablock'.'@datablockName'");
        si.setTransformScript("transform/pdb.trs");
        si.setContentSpecParam("ftpHost", "ftp.wwpdb.org");
        si.setContentSpecParam("remotePath", "/pub/pdb/data/structures/divided/XML/");
        si.setContentSpecParam("filenamePattern", ".+\\.xml\\.gz$");
        si.setContentSpecParam("recursive", "true");
        si.setContentSpecParam("outDir", "/var/temp/pdb_ftp");
        si.setContentSpecParam("documentElement", "datablock");
        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/pdb.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }

    public void testPDBSourceGenerationRsync() throws Exception {
        SourceInfo si = new SourceInfo("nif-0000-00135",
                "RCSB Protein Data Bank", "nif-0000-00135", IngestMethod.RSYNC);

        // primary key for PDB
        si.setPrimaryKeyJsonPath("$.'PDBx:datablock'.'@datablockName'");
        si.setTransformScript("transform/pdb.trs");
        si.setContentSpecParam("rsyncSource", "rsync.wwpdb.org::ftp_data/structures/divided/XML-noatom/");
        si.setContentSpecParam("rsyncDest", "/var/temp/pdb_rsync");
        si.setContentSpecParam("filenamePattern", ".+\\.xml\\.gz$");
        si.setContentSpecParam("largeRecords", "true");
        si.setContentSpecParam("port", "33444");
        si.setContentSpecParam("documentElement", "datablock");
        si.setContentSpecParam("fullSet", "false");
        File outFile = new File("/var/burak/dev/java/Foundry-ES/consumers/etc/pdb_rsync_gen.json");
        SourceDescFileGenerator.prepareSource(si, outFile);
    }
}
