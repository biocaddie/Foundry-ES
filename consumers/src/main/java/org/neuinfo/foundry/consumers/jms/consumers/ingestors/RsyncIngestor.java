package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.command.CommandFactory;
import org.neuinfo.foundry.common.model.CommandInput;
import org.neuinfo.foundry.common.model.ICommand;
import org.neuinfo.foundry.common.model.ICommandOutput;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.LFTPWrapper;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 5/13/15.
 */
public class RsyncIngestor implements Ingestor {
    String rsyncSource;
    String rsyncDest;
    String fileNamePattern;
    String rsyncIncludePattern;
    String preprocessCommand;
    String surroundingTag;
    String topElName;
    String docElName;
    Map<String, String> optionMap;
    boolean testMode = false;
    int maxNumDocs2Ingest = -1;
    boolean largeRecords = false;
    boolean sampleMode = false;
    int sampleSize = 1;
    /**
     * if true regardless what is changed during the rsync all local files are processed.
     */
    boolean fullSet = false;
    int port = -1;
    // Iterator<File> fileIterator;
    int numOfRecords = -1;
    int count = 0;
    XMLFileIterator xmlFileIterator;
    static Logger log = Logger.getLogger(RsyncIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.rsyncSource = options.get("rsyncSource");
        this.rsyncDest = options.get("rsyncDest");
        this.fileNamePattern = options.get("filenamePattern");
        this.rsyncIncludePattern = options.containsKey("rsyncIncludePattern") ?
                options.get("rsyncIncludePattern") : null;
        this.preprocessCommand = options.containsKey("preprocessCommand") ?
                options.get("preprocessCommand") : null;
        this.surroundingTag = options.containsKey("surroundingTag") ?
                options.get("surroundingTag") : null;
        this.topElName = options.containsKey("topElement") ?
                options.get("topElement") : null;
        this.docElName = options.get("documentElement");
        this.largeRecords = options.containsKey("largeRecords")
                ? Boolean.parseBoolean(options.get("largeRecords")) : false;
        this.fullSet = options.containsKey("fullSet") ?
                Boolean.parseBoolean(options.get("fullSet")) : false;
        if (options.containsKey("maxDocs")) {
            this.maxNumDocs2Ingest = Utils.getIntValue(options.get("maxDocs"), -1);
        }
        if (options.containsKey("testMode")) {
            this.testMode = Boolean.parseBoolean(options.get("testMode"));
        }
        if (options.containsKey("sampleMode")) {
            this.sampleMode = Boolean.parseBoolean(options.get("sampleMode"));
        }
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        if (options.containsKey("port")) {
            this.port = Utils.getIntValue(options.get("port"), -1);
        }
        this.optionMap = options;
    }

    @Override
    public void startup() throws Exception {
        Assertion.assertNotNull(this.rsyncSource, "rsyncSource");
        Assertion.assertNotNull(this.rsyncDest, "rsyncDest");
        if (!new File(rsyncDest).isDirectory()) {
            new File(rsyncDest).mkdirs();
        }
        List<String> cmdList = new ArrayList<String>(10);
        cmdList.add("rsync");
        //  cmdList.add("-rlpt -i  -z --delete --port=33444");
        cmdList.add("-rlpt");
        cmdList.add("-i");
        cmdList.add("-z");
        cmdList.add("--delete");
        if (port > 0) {
            cmdList.add("--port=" + port);
        }
        if (this.rsyncIncludePattern != null) {
            cmdList.add("--include=*/");
            cmdList.add("--include=" + this.rsyncIncludePattern);
            cmdList.add("--exclude=*");
        }
        cmdList.add(this.rsyncSource);
        cmdList.add(this.rsyncDest);

        log.info(cmdList.toString());
        List<File> changedFiles = null;

        if (!this.testMode) {
            changedFiles = handleRsync(cmdList);
        } else {
            changedFiles = new LinkedList<File>();
        }
        if (this.fullSet) {
            changedFiles.clear();
            changedFiles = Utils.findAllFilesMatching(new File(rsyncDest),
                    new Utils.RegexFileNameFilter(fileNamePattern));
            if (sampleMode) {
                changedFiles = changedFiles.subList(0, sampleSize);
            }
        }

        if (this.preprocessCommand != null) {
            ICommand processor = CommandFactory.create(this.preprocessCommand);
            Map<String, List<File>> fileGroupMap = groupFilesInLeafDirectories(changedFiles);
            List<File> processedFiles = new LinkedList<File>();
            for (List<File> fileGroups : fileGroupMap.values()) {
                CommandInput input = new CommandInput(fileGroups);
                if (this.surroundingTag != null) {
                    input.setParam("surroundingTag", this.surroundingTag);
                }
                ICommandOutput output = processor.handle(input);
                if (output != null && !output.getFiles().isEmpty()) {
                    processedFiles.add(output.getFiles().get(0));
                }
            }
            changedFiles = processedFiles;
        }

        // this.fileIterator = changedFiles.iterator();
        //this.numOfRecords = changedFiles.size();
        this.numOfRecords = -1;

        this.xmlFileIterator = new XMLFileIterator(
                new RemoteFileIterator(changedFiles), this.topElName, this.docElName);
        log.info("numOfFiles:" + changedFiles.size());
    }

    List<File> handleRsync(List<String> cmdList) throws Exception {
        if (sampleMode) {
            String sourceURL = rsyncSource.replaceFirst("::", "/");
            File parentDir = new File(rsyncDest).getParentFile();
            File stageDir = new File(parentDir, "stage");
            stageDir.mkdir();
            File outDir = new File(stageDir, new File(rsyncDest).getName());
            outDir.mkdir();
            Assertion.assertTrue(outDir.isDirectory());
            LFTPWrapper lftp = new LFTPWrapper(sourceURL, this.fileNamePattern);
            List<File> samples = lftp.sample(this.sampleSize, outDir.getAbsolutePath());
            return samples;
        }
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        System.out.println(pb.command());
        Process process = pb.start();
        BufferedReader bin = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        String line;
        List<File> changedFiles = new LinkedList<File>();
        Pattern pattern = null;
        if (fileNamePattern != null) {
            pattern = Pattern.compile(fileNamePattern);
        }
        int i = 0;
        while ((line = bin.readLine()) != null) {
            if (line.startsWith(">")) {
                log.info(line);
                int idx = line.lastIndexOf(" ");
                if (idx != -1) {
                    String changedFile = line.substring(idx + 1);
                    File f = new File(this.rsyncDest, changedFile);
                    if (pattern != null) {
                        Matcher m = pattern.matcher(changedFile);
                        if (m.find()) {
                            changedFiles.add(f);
                            i++;
                            if (!canContinue(i)) {
                                break;
                            }
                        }
                    } else {
                        changedFiles.add(f);
                        i++;
                        if (!canContinue(i)) {
                            break;
                        }
                    }
                }
            }
        }

        int rc = process.waitFor();
        log.info("rc:" + rc);
        log.info("Changed files");
        for (File f : changedFiles) {
            if (f.isFile()) {
                log.info(f);
            }
        }
        return changedFiles;
    }

    private Map<String, List<File>> groupFilesInLeafDirectories(List<File> changedFiles) {
        Map<String, List<File>> map = new LinkedHashMap<String, List<File>>();
        for (File f : changedFiles) {
            String key = f.getParent();
            List<File> leafFiles = map.get(key);
            if (leafFiles == null) {
                leafFiles = new LinkedList<File>();
                map.put(key, leafFiles);
            }
            leafFiles.add(f);
        }
        return map;
    }

    private boolean canContinue(int curCount) {
        if (!sampleMode) {
            return true;
        }
        return curCount < sampleSize;
    }

    @Override
    public Result prepPayload() {
        try {
            Element el = this.xmlFileIterator.next();
            /*
            Element rootEl;
            if (xmlFile.getName().endsWith(".gz")) {
                rootEl = Utils.loadGzippedXML(xmlFile.getAbsolutePath());
            } else {
                rootEl = Utils.loadXML(xmlFile.getAbsolutePath());
            }

            Element el = extractXmlRecord(rootEl);
            */
            XML2JSONConverter converter = new XML2JSONConverter();
            JSONObject json = converter.toJSON(el);
            count++;
            Result r = new Result(json, Result.Status.OK_WITH_CHANGE);
            r.setLargeRecord(this.largeRecords);
            File xmlFile = xmlFileIterator.getCurFile();
            r.setCachedFileName(xmlFile.getAbsolutePath());
            return r;
        } catch (Throwable t) {
            log.error("prepPayload", t);
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
        return "RsyncIngestor";
    }

    @Override
    public int getNumRecords() {
        return this.numOfRecords;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean hasNext() {
        if (this.maxNumDocs2Ingest > 0 && this.count >= this.maxNumDocs2Ingest) {
            return false;
        }
        return this.xmlFileIterator.hasNext();
    }
}
