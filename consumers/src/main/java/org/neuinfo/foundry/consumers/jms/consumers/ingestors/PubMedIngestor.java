package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.FileExpander;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.common.LFTPWrapper;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/18/16.
 */
public class PubMedIngestor implements Ingestor {
    String ingestURL;
    String opMode = "full";
    String updateURL;
    String topElName;
    String docElName;
    String fileNamePattern;
    boolean useCache = true;
    Map<String, String> optionMap;
    boolean sampleMode = false;
    int sampleSize = 1;
    XMLFileIterator xmlFileIterator;
    static Logger log = Logger.getLogger(PubMedIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.ingestURL = options.get("ingestURL");
        this.updateURL = options.get("updateURL");
        this.docElName = options.get("documentElement");
        this.topElName = options.containsKey("topElement") ? options.get("topElement") : null;
        this.fileNamePattern = options.get("fileNamePattern");

        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
    }

    @Override
    public void startup() throws Exception {
        Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");

        if (opMode.equals("full")) {
            LFTPWrapper lftp = new LFTPWrapper(ingestURL, this.fileNamePattern);
            String topDirName = Utils.fromURL2FileName(ingestURL);
            if (sampleMode) {
                File sampleCacheRoot = new File(cacheRoot,"sample");
                sampleCacheRoot.mkdirs();
                File outDir = new File(sampleCacheRoot, topDirName);
                outDir.mkdirs();
                List<File> sample = lftp.sample(1, outDir.getAbsolutePath());
                this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(sample),
                        this.topElName, this.docElName);
            } else {
                File outDir = new File(cacheRoot, topDirName);
                outDir.mkdirs();

                List<File> files = lftp.mirror(this.fileNamePattern, outDir.getAbsolutePath());
                RemoteFileIterator rfi = new RemoteFileIterator(files);
                this.xmlFileIterator = new XMLFileIterator(rfi, this.topElName, this.docElName);
            }
        }

    }

    @Override
    public Result prepPayload() {
        try {
            Element element = xmlFileIterator.next();
            Result r = ConsumerUtils.convert2JSON(element);
            // FIXME check for deleted record
            return r;
        } catch(Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }

    @Override
    public String getName() {
        return "PubMedIngestor";
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
