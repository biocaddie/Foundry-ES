package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.FileExpander;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.common.JSONFileIterator;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 12/18/15.
 */
public class WebIngestor implements Ingestor {
    String ingestURL;
    String cacheFilename;
    String fileNamePattern;
    String docElName;
    String topElName;
    String parserType = "xml";
    boolean useCache = true;
    XMLFileIterator xmlFileIterator;
    JSONFileIterator jsonFileIterator;
    Map<String, String> optionMap;
    int count = 0;
    String offsetParam;
    String limitParam;
    int limitValue = 100;

    boolean sampleMode = false;
    int sampleSize = 1;
    static Logger log = Logger.getLogger(WebIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.ingestURL = options.get("ingestURL");
        this.cacheFilename = options.get("cacheFilename");
        this.fileNamePattern = options.get("filenamePattern");
        this.parserType = options.containsKey("parserType") ? options.get("parserType") : "xml";
        this.docElName = options.get("documentElement");
        this.topElName = options.containsKey("topElement") ? options.get("topElement") : null;
        if (options.containsKey("offsetParam")) {
            this.offsetParam = options.get("offsetParam");
        }
        if (options.containsKey("limitParam")) {
            this.limitParam = options.get("limitParam");
        }
        if (options.containsKey("limitValue")) {
            this.limitValue = Utils.getIntValue(options.get("limitValue"), -1);
        }
        this.optionMap = options;
        this.useCache = options.containsKey("useCache") ?
                Boolean.parseBoolean(options.get("useCache")) : true;
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        Assertion.assertTrue(this.parserType.equals("xml") || this.parserType.equals("json"));
    }

    @Override
    public void startup() throws Exception {
        log.info("getting content from " + ingestURL + " using cache:" + useCache);
        if (offsetParam != null) {
            ContentLoader loader = new ContentLoader(offsetParam);
            List<File> contentFiles = new LinkedList<File>();
            while (true) {
                File rawData = loader.getNextContentPage(ingestURL, cacheFilename, limitParam, limitValue, useCache);
                contentFiles.add(rawData);
                if (parserType.equals("xml")) {
                    XMLFileIterator xfi = new XMLFileIterator(new RemoteFileIterator(Arrays.asList(rawData)),
                            this.topElName, this.docElName);
                    int count = 0;
                    while (xfi.hasNext()) {
                        xfi.next();
                        count++;
                    }
                    loader.incrOffset(count);
                    if (count < limitValue) {
                        break;
                    }
                } else {
                    JSONFileIterator jfi = new JSONFileIterator(new RemoteFileIterator(Arrays.asList(rawData)),
                            this.docElName);
                    int count = 0;
                    while (jfi.hasNext()) {
                        jfi.next();
                        count++;
                    }
                    loader.incrOffset(count);
                    if (count < limitValue) {
                        break;
                    }
                }
            }
            if (parserType.equals("xml")) {
                this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(contentFiles),
                        this.topElName, this.docElName);
            } else {
                this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(contentFiles), this.docElName);
            }

        } else {
            File rawData = ContentLoader.getContent(ingestURL, cacheFilename, useCache);
            log.info("got content from " + ingestURL);
            FileExpander expander = new FileExpander();
            File expandedData = expander.expandIfNecessary(rawData, this.useCache);
            Assertion.assertNotNull(expandedData);
            if (expandedData.isDirectory()) {
                Utils.RegexFileNameFilter fileNameFilter = new Utils.RegexFileNameFilter(this.fileNamePattern);
                List<File> fileList = Utils.findAllFilesMatching(expandedData, fileNameFilter);
                if (parserType.equals("xml")) {
                    this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(fileList),
                            this.topElName, this.docElName);
                } else {
                    this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(fileList), this.docElName);
                    // throw new RuntimeException("JSON multiple file support is not implemented yet!");
                }
            } else if (expandedData.isFile()) {
                if (parserType.equals("xml")) {
                    this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(Arrays.asList(expandedData)),
                            this.topElName, this.docElName);
                } else {
                    this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(Arrays.asList(expandedData)),
                            this.docElName);
                }
            }
        }

    }

    @Override
    public Result prepPayload() {
        try {
            Result r;
            if (this.parserType.equals("xml")) {
                Element el = xmlFileIterator.next();
                r = ConsumerUtils.convert2JSON(el);
            } else {
                JSONObject json = jsonFileIterator.next();
                return new Result(json, Result.Status.OK_WITH_CHANGE);
            }
            count++;
            return r;
        } catch (Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }

    @Override
    public String getName() {
        return "WebIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
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
        if (this.parserType.equals("xml")) {
            return xmlFileIterator.hasNext();
        } else {
            return jsonFileIterator.hasNext();
        }
    }
}
