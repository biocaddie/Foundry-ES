package org.neuinfo.foundry.consumers.utils;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by bozyurt on 10/31/15.
 */
public class XMLIngestionTestCLI {
    XMLFileIterator xmlFileIterator;
    String dataRootDir;
    int sampleSize = -1;
    File theXMLFile;
    static Logger log = Logger.getLogger(XMLIngestionTestCLI.class);

    public XMLIngestionTestCLI(String dataRootDir, int sampleSize, File theXMLFile) {
        this.dataRootDir = dataRootDir;
        this.sampleSize = sampleSize;
        this.theXMLFile = theXMLFile;
    }

    public void startup() throws Exception {
        List<File> changedFiles = null;
        if (theXMLFile != null) {
            changedFiles = Arrays.asList(theXMLFile);
        } else {
            changedFiles = Utils.findAllFilesMatching(new File(dataRootDir),
                    new Utils.RegexFileNameFilter("\\.xml$"));
            if (sampleSize > 0) {
                changedFiles = changedFiles.subList(0, sampleSize);
            }
        }
        this.xmlFileIterator = new XMLFileIterator(
                new RemoteFileIterator(changedFiles), null, "datablock");
        log.info("numOfFiles:" + changedFiles.size());
    }

    public void handle() {
        while (xmlFileIterator.hasNext()) {
            try {
                Element el = this.xmlFileIterator.next();
                // int memoryUsedInBytes = Utils.sizeOf(el);
                // System.out.println("memoryUsedInBytes: "  + memoryUsedInBytes);
                XML2JSONConverter converter = new XML2JSONConverter();
                JSONObject json = converter.toJSON(el);
                log.info("converted to json: " + xmlFileIterator.getCurFile());
                Utils.saveText(json.toString(2), "/tmp/" + xmlFileIterator.getCurFile().getName());

            } catch (Throwable t) {
                log.error("handle", t);
                t.printStackTrace();
            }
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("XMLIngestionTestCLI", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option sampleSizeOption = Option.builder("n").argName("sampleSize")
                .hasArg().build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(sampleSizeOption);
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

        int sampleSize = -1;
        String dataRoot = "/var/temp/pdb_rsync";
        File theXMLFile = new File("/var/temp/pdb_rsync/ud/4udf-noatom.xml");
        if (line.hasOption('n')) {
            sampleSize = Utils.getIntValue(line.getOptionValue('n'), -1);
            if (sampleSize != -1) {
                theXMLFile = null;
            }
        }

        log.info("===================== XMLIngestionTestCLI start ==================================");
        XMLIngestionTestCLI xit = new XMLIngestionTestCLI(dataRoot, sampleSize, theXMLFile);

        xit.startup();
        xit.handle();
        log.info("===================== XMLIngestionTestCLI end ==================================");
    }
}
