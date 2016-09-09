package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.common.IngestFileCacheManager;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bozyurt on 11/5/14.
 */
public class OAIIngestor implements Ingestor {
    String ingestURL;
    String sourceName;
    boolean allowDuplicates = false;
    String topElName;
    String docElName;
    boolean useCache = false;
    boolean testMode = false;
    boolean sampleMode = false;
    int sampleSize = 1;
    String metadataPrefix;
    Set<String> allowedSetSpecs;
    Map<String, String> optionMap;
    XMLFileIterator xmlFileIterator;
    static Logger log = Logger.getLogger(OAIIngestor.class);


    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.ingestURL = options.get("ingestURL");
        this.sourceName = options.get("sourceName");
        this.allowDuplicates = options.containsKey("allowDuplicates") ?
                Boolean.parseBoolean(options.get("allowDuplicates")) : false;
        this.topElName = options.get("topElement");
        this.docElName = options.get("documentElement");
        if (options.containsKey("testMode")) {
            this.testMode = Boolean.parseBoolean(options.get("testMode"));
        }
        if (options.containsKey("sampleMode")) {
            this.sampleMode = Boolean.parseBoolean(options.get("sampleMode"));
        }
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        if (options.containsKey("useCache")) {
            this.useCache = Boolean.parseBoolean( options.get("useCache"));
        }
        if (options.containsKey("allowedSetSpecs")) {
            String str = options.get("allowedSetSpecs");
            String[] tokens = str.split("\\s*,\\s*");
            if (tokens.length > 0) {
                allowedSetSpecs = new HashSet<String>();
                for (String token : tokens) {
                    allowedSetSpecs.add(token.trim());
                }
            }
        }
        metadataPrefix = options.containsKey("metadataPrefix") ? options.get("metadataPrefix") : null;
    }

    @Override
    public void startup() throws Exception {
        Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");
        String cacheDir = null;
        Utils.RegexFileNameFilter fileNameFilter = new Utils.RegexFileNameFilter("\\_complete.xml$");
        IngestFileCacheManager cacheManager = IngestFileCacheManager.getInstance();
        List<File> fileList =  null;
        if (useCache) {
            File file = cacheManager.get(sourceName + ".cacheDir");
            if (file != null && file.isDirectory()) {
                fileList = Utils.findAllFilesMatching(new File(cacheDir), fileNameFilter);
                if (!fileList.isEmpty()) {
                    cacheDir = file.getAbsolutePath();
                }
            }
        }
        if (cacheDir == null) {
            OAIPMHHarvester harvester = new OAIPMHHarvester(sourceName, ingestURL, cacheRoot, sampleMode);
            if (metadataPrefix != null) {
                harvester.setMetaDataPrefix(metadataPrefix);
            }
            if (allowedSetSpecs != null && !allowedSetSpecs.isEmpty()) {
                harvester.setAllowedSetSpecs(allowedSetSpecs);
            }

            cacheDir = harvester.handle(testMode);
            cacheManager.put(sourceName + ".cacheDir", new File(cacheDir));
            fileList = Utils.findAllFilesMatching(new File(cacheDir), fileNameFilter);
        }

        this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(fileList),
                this.topElName, this.docElName);

    }

    @Override
    public Result prepPayload() {
        try {
            Element el = xmlFileIterator.next();
            return ConsumerUtils.convert2JSON(el);
        } catch (Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }

    @Override
    public String getName() {
        return "OAIIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
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
        return xmlFileIterator.hasNext();
    }
}
