package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.common.JSONFileIterator;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

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

    String idJsonPath;
    String mergeIngestURLTemplate;
    String mergeFieldName;
    List<String> templateVariables;
    String mergeDocElName;
    String filterJsonPath;
    String filterValue;
    boolean normalize = false;

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

        if (options.containsKey("mergeIngestURLTemplate")) {
            mergeIngestURLTemplate = options.get("mergeIngestURLTemplate");
        }
        if (options.containsKey("idJsonPath")) {
            this.idJsonPath = options.get("idJsonPath");
        }
        if (options.containsKey("mergeFieldName")) {
            this.mergeFieldName = options.get("mergeFieldName");
        }
        this.mergeDocElName = options.containsKey("mergeDocElName") ? options.get("mergeDocElName") : this.docElName;

        if (options.containsKey("filterJsonPath")) {
            this.filterJsonPath = options.get("filterJsonPath");
        }
        if (options.containsKey("filterValue")) {
            this.filterValue = options.get("filterValue");
        }


        this.optionMap = options;
        this.normalize = options.containsKey("normalize") ? Boolean.parseBoolean(options.get("normalize")) : false;
        this.useCache = options.containsKey("useCache") ?
                Boolean.parseBoolean(options.get("useCache")) : true;
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        Assertion.assertTrue(this.parserType.equals("xml") || this.parserType.equals("json"));

        if (mergeIngestURLTemplate != null) {
            this.templateVariables = IngestorHelper.extractTemplateVariables(this.mergeIngestURLTemplate);
            Assertion.assertNotNull(this.templateVariables);
            Assertion.assertTrue(this.templateVariables.size() == 1);
            Assertion.assertNotNull(this.idJsonPath);
            Assertion.assertNotNull(this.mergeFieldName);
            Assertion.assertNotNull(this.mergeDocElName);
        }
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
                            this.docElName, this.filterJsonPath, this.filterValue);
                    int count = 0;
                    if (this.filterValue != null) {
                        count = jfi.getNonFilterCount();
                    } else {
                        while (jfi.hasNext()) {
                            jfi.next();
                            count++;
                        }
                    }
                    loader.incrOffset(count);
                    if (count < limitValue) {
                        break;
                    }
                }
                if (sampleMode && loader.curOffset >= sampleSize) {
                    break;
                }
            }
            if (parserType.equals("xml")) {
                this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(contentFiles),
                        this.topElName, this.docElName);
            } else {
                this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(contentFiles), this.docElName,
                        this.filterJsonPath, this.filterValue);
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
                    this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(fileList),
                            this.docElName, this.filterJsonPath, this.filterValue);
                }
            } else if (expandedData.isFile()) {
                if (parserType.equals("xml")) {
                    this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(Arrays.asList(expandedData)),
                            this.topElName, this.docElName);
                } else {
                    this.jsonFileIterator = new JSONFileIterator(new RemoteFileIterator(Arrays.asList(expandedData)),
                            this.docElName, this.filterJsonPath, this.filterValue);
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
                r = ConsumerUtils.convert2JSON(el, normalize);
            } else {
                JSONObject json = jsonFileIterator.next();
                if (mergeIngestURLTemplate != null) {
                    mergeRelatedDoc(json);
                }
                if (normalize) {
                    JSONUtils.normalize(json);
                }

                return new Result(json, Result.Status.OK_WITH_CHANGE);
            }
            count++;
            return r;
        } catch (Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }

    void mergeRelatedDoc(JSONObject json) throws Exception {
        JSONPathProcessor2 pathProcessor2 = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> jpNodes = pathProcessor2.find(this.idJsonPath, json);
        if (jpNodes != null && !jpNodes.isEmpty()) {
            String idValue = jpNodes.get(0).getValue();
            Map<String, String> templateVar2ValueMap = new HashMap<String, String>(3);
            templateVar2ValueMap.put(templateVariables.get(0), idValue);
            String url = IngestorHelper.createURL(this.mergeIngestURLTemplate, templateVar2ValueMap);
            String jsonContent = Utils.sendGetRequest(url);
            JSONObject js = new JSONObject(jsonContent);
            pathProcessor2 = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> jpNodes1 = pathProcessor2.find("$.." + this.mergeDocElName, js);
            if (jpNodes1 != null && !jpNodes1.isEmpty()) {
                Object payload = jpNodes1.get(0).getPayload();
                json.put(this.mergeFieldName, payload);
            }
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
