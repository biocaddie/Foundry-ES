package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;

/**
 * <pre>
 *     ascp -i /home/bozyurt/.aspera/connect/etc/asperaweb_id_dsa.openssh -k1 -Tr -l100m anonftp@ftp.ncbi.nlm.nih.gov:/geo/platforms/ /var/data/geo
 * </pre>
 * Created by bozyurt on 7/23/15.
 */
public class AsperaIngestor implements Ingestor {
    String source;
    String dest;
    String args;
    String fileNamePattern;
    String publicKeyFile;
    String docElName;
    String parserType = "xml";
    boolean largeRecords = false;
    Map<String, String> optionMap;
    Iterator<File> fileIterator;
    int numOfRecords = -1;
    int maxNumDocs2Ingest = -1;
    int count = 0;
    boolean sampleMode = false;
    int sampleSize = 1;
    boolean fullSet = false;
    String xmlFileNamePattern;
    static Logger log = Logger.getLogger(AsperaIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.source = options.get("source");
        this.dest = options.get("dest");
        this.publicKeyFile = options.get("publicKeyFile");
        this.args = options.get("arguments");
        this.docElName = options.get("documentElement");
        this.largeRecords = options.containsKey("largeRecords")
                ? Boolean.parseBoolean(options.get("largeRecords")) : false;
        this.fullSet = options.containsKey("fullSet")
                ? Boolean.parseBoolean(options.get("fullSet")) : false;
        this.fileNamePattern = options.get("filenamePattern");
        this.xmlFileNamePattern = options.containsKey("xmlFileNamePattern") ?
                options.get("xmlFileNamePattern") : null;
        this.parserType = options.get("parserType");
        if (this.parserType == null) {
            this.parserType = "xml";
        }
        this.optionMap = options;
        if (this.parserType.equals("xml")) {
            Assertion.assertNotNull(this.docElName);
        }
        if (options.containsKey("sampleMode")) {
            this.sampleMode = Boolean.parseBoolean(options.get("sampleMode"));
        }
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
    }

    @Override
    public void startup() throws Exception {
        Assertion.assertNotNull(this.source);
        Assertion.assertNotNull(this.dest);
        Assertion.assertNotNull(this.publicKeyFile);
        if (!new File(dest).isDirectory()) {
            new File(dest).mkdirs();
        }
        List<String> cmdList = new ArrayList<String>(10);
        //FIXME
        String HOME_DIR = System.getProperty("user.home");
        cmdList.add(HOME_DIR + "/.aspera/connect/bin/ascp");
        cmdList.add("-i");
        cmdList.add(this.publicKeyFile);
        if (this.args != null) {
            String[] toks = args.split("\\s+");
            for (String tok : toks) {
                cmdList.add(tok);
            }
        }
        cmdList.add(this.source);
        cmdList.add(this.dest);
        if (log.isInfoEnabled()) {
            StringBuilder cmdSB = new StringBuilder(200);
            for (String cmd : cmdList) {
                cmdSB.append(cmd).append(' ');
            }
            log.info(cmdSB.toString().trim());
        }
        if (!fullSet) {
            ProcessBuilder pb = new ProcessBuilder(cmdList);

            Process process = pb.start();
            BufferedReader bin = new BufferedReader(new InputStreamReader(
                    process.getInputStream()));
            String line;
            while ((line = bin.readLine()) != null) {
                System.out.println(line);
            }
            int rc = process.waitFor();
            bin.close();
        }

        List<File> changedFiles = Utils.findAllFilesMatching(new File(dest),
                new Utils.RegexFileNameFilter(fileNamePattern));
        if (sampleMode) {
            changedFiles = changedFiles.subList(0, sampleSize);
        }
        this.fileIterator = changedFiles.iterator();
        this.numOfRecords = changedFiles.size();
        log.info("numOfRecords:" + numOfRecords);
    }


    @Override
    public Result prepPayload() {
        try {
            if (this.parserType != null && !this.parserType.equals("xml")) {
                File file = this.fileIterator.next();
                if (file.getName().endsWith(".gz")) {
                    File destFile = new File(file.getAbsoluteFile().getAbsolutePath().replaceFirst("\\.gz$", ""));
                    Utils.extractGzippedFile(file.getAbsolutePath(), destFile);
                    if (destFile.isFile()) {
                        InputDataIterator idIter = new InputDataIterator(destFile);
                        IDataParser parser = DataParserFactory.getInstance().createDataParser(this.parserType, idIter);
                        JSONObject json = parser.toJSON();
                        count++;
                        Result r = new Result(json, Result.Status.OK_WITH_CHANGE);
                        destFile.delete();
                        return r;

                    } else {
                        return new Result(null, Result.Status.ERROR, "");
                    }
                }
                return new Result(null, Result.Status.ERROR, "");
            } else {
                File xmlFile = this.fileIterator.next();
                Element rootEl;
                if (xmlFile.getName().endsWith(".gz")) {
                    rootEl = Utils.loadGzippedXML(xmlFile.getAbsolutePath());
                } else if (xmlFile.getName().endsWith(".tgz")) {
                    Assertion.assertNotNull(xmlFileNamePattern);
                    rootEl = Utils.extractXMLFromTar(xmlFile.getAbsolutePath(), xmlFileNamePattern);

                } else {
                    rootEl = Utils.loadXML(xmlFile.getAbsolutePath());
                }
                Element el = extractXmlRecord(rootEl);
                XML2JSONConverter converter = new XML2JSONConverter();
                JSONObject json = converter.toJSON(el);
                count++;
                Result r = new Result(json, Result.Status.OK_WITH_CHANGE);
                return r;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            return new Result(null, Result.Status.ERROR, t.getMessage());
        }
    }

    Element extractXmlRecord(Element rootEl) throws Exception {
        if (rootEl.getName().equals(this.docElName)) {
            return rootEl;
        }
        return null;
    }

    @Override
    public String getName() {
        return "AsperaIngestor";
    }

    @Override
    public int getNumRecords() {
        return this.numOfRecords;
    }

    @Override
    public String getOption(String optionName) {
        return this.optionMap.get(optionName);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean hasNext() {
        if (this.maxNumDocs2Ingest > 0 && this.count >= this.maxNumDocs2Ingest) {
            return false;
        }
        return this.fileIterator.hasNext();
    }
}
