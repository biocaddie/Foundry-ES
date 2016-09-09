package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.HtmlEntityUtil;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.CSVFileIterator;
import org.neuinfo.foundry.consumers.common.IngestFileCacheManager;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.*;

/**
 * Created by bozyurt on 8/4/15.
 */
public class ResourceIngestor implements Ingestor {
    Map<String, String> optionMap;
    String srcUrl;
    String urlTemplate;
    String fileType;
    int ignoreLines;
    int headerLine;
    String delimiter;
    String textQuote;
    String escapeChar;
    String localeStr;
    String docElName;
    String topElName;
    CSVFileIterator csvRecordsIter;
    XMLFileIterator xmlFileIterator;
    IngestFileCacheManager cacheMan;

    List<String> currentRow;
    Element currentElement;
    int recordIdx = 0;
    int maxNumDocs2Ingest = -1;
    boolean sampleMode = false;
    int sampleSize = 1;
    Map<String, HeaderInformation> hiMap = new HashMap<String, HeaderInformation>();
    public final static String CSV = "csv";
    public final static String XML = "xml";

    /**
     * any mappings of fields to be injected into the JSON version of the original record
     */
    Map<String, FieldIdx> fieldMapping = new LinkedHashMap<String, FieldIdx>(7);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.srcUrl = options.get("sourceURL");
        this.urlTemplate = options.get("urlTemplate");
        this.fileType = options.get("fileType");
        Assertion.assertTrue(this.fileType.equals("csv") || this.fileType.equals("xml"));
        Assertion.assertTrue(IngestorHelper.isDocumentServiceURL(this.srcUrl), "Not a valid resource url");
        this.ignoreLines = Utils.getIntValue(options.get("ignoreLines"), -1); // one based
        this.headerLine = Utils.getIntValue(options.get("headerLine"), -1); // one based
        this.delimiter = options.containsKey("delimiter") ? options.get("delimiter") : ",";
        this.textQuote = options.get("textQuote");
        this.escapeChar = options.get("escapeCharacter");
        this.localeStr = options.get("locale");
        if (fileType.equals(XML)) {
            this.docElName = options.get("documentElement");
            this.topElName = options.get("topElement");
        }
        if (options.containsKey("maxDocs")) {
            this.maxNumDocs2Ingest = Utils.getIntValue(options.get("maxDocs"), -1);
        }
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        if (options.containsKey("fieldsToAdd")) {
            String s = options.get("fieldsToAdd");
            String[] toks = s.split("\\s*,\\s*");
            int i = 0;
            for (String tok : toks) {
                int idx = tok.indexOf(':');
                if (idx == -1) {
                    this.fieldMapping.put(tok, new FieldIdx(tok, i));
                } else {
                    String origName = tok.substring(0, idx);
                    String newName = tok.substring(idx + 1);
                    this.fieldMapping.put(origName, new FieldIdx(newName, i));
                }
                i++;
            }
        }

        if (fileType.equals(CSV)) {
            if (HtmlEntityUtil.isHtmlEntity(this.textQuote)) {
                this.textQuote = HtmlEntityUtil.getChar(this.textQuote).toString();
            }
            if (HtmlEntityUtil.isHtmlEntity(this.escapeChar)) {
                this.escapeChar = HtmlEntityUtil.getChar(this.escapeChar).toString();
            }
            if (HtmlEntityUtil.isHtmlEntity(this.delimiter)) {
                this.delimiter = HtmlEntityUtil.getChar(this.delimiter).toString();
            }
        }
    }

    @Override
    public void startup() throws Exception {
        handleStartup();
    }

    private void handleStartup() throws Exception {
        this.cacheMan = IngestFileCacheManager.getInstance();

        Map<String, List<String>> sourceData = IngestorHelper.getSourceData(this.srcUrl, "nifRecords");

        List<String> templateVariables = IngestorHelper.extractTemplateVariables(this.urlTemplate);
        Assertion.assertNotNull(templateVariables);

        RemoteFileIterator rfi = new RemoteFileIterator(sourceData, urlTemplate, fieldMapping);
        rfi.setCacheManager(this.cacheMan);
        if (this.fileType.equals(CSV)) {

            this.csvRecordsIter = new CSVFileIterator(rfi, this.delimiter);
        } else if (this.fileType.equals(XML)) {
            this.xmlFileIterator = new XMLFileIterator(rfi, this.topElName, this.docElName);
        } else {
            throw new RuntimeException("Unsupported file type:" + fileType);
        }
    }

    @Override
    public Result prepPayload() {
        if (fileType.equals(CSV)) {

            this.currentRow = this.csvRecordsIter.next();

            String[] addedFields = csvRecordsIter.getAddedFieldsForCurrentSourceRec();

            this.recordIdx++;

            if (this.currentRow != null) {
                JSONObject json = new JSONObject();
                String key = HeaderInformation.prepKey(addedFields);
                HeaderInformation hi = this.hiMap.get(key);
                Assertion.assertNotNull(hi);
                String[] headerCols = hi.getHeaderCols();
                for (int i = 0; i < headerCols.length; i++) {
                    if (this.currentRow.size() <= i) {
                        json.put(headerCols[i], "");
                        // System.out.println("More record fields than header fields at recordIdx:" + recordIdx);
                    } else {
                        String value = this.currentRow.get(i);
                        json.put(headerCols[i], value);
                    }
                }
                if (addedFields != null) {
                    for (String origFieldName : fieldMapping.keySet()) {
                        FieldIdx fi = fieldMapping.get(origFieldName);
                        json.put(fi.fieldName, addedFields[fi.idx]);
                    }
                }

                return new Result(json, Result.Status.OK_WITH_CHANGE);
            }
        } else if (fileType.equals(XML)) {
            this.currentElement = this.xmlFileIterator.next();
            String[] addedFields = xmlFileIterator.getAddedFieldsForCurrentSourceRec();
            this.recordIdx++;
            if (this.currentElement != null) {
                try {
                    XML2JSONConverter converter = new XML2JSONConverter();
                    JSONObject json = converter.toJSON(this.currentElement);
                    if (addedFields != null) {
                        for (String origFieldName : fieldMapping.keySet()) {
                            FieldIdx fi = fieldMapping.get(origFieldName);
                            json.put(fi.fieldName, addedFields[fi.idx]);
                        }
                    }
                    return new Result(json, Result.Status.OK_WITH_CHANGE);
                } catch (Throwable t) {
                    t.printStackTrace();
                    return new Result(null, Result.Status.ERROR, t.getMessage());
                }
            }
        }
        return null;
    }

    @Override
    public String getName() {
        return "ResourceIngestor";
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
        if (cacheMan != null) {
            cacheMan.shutdown();
        }
    }

    @Override
    public boolean hasNext() {
        if (this.maxNumDocs2Ingest > 0 && this.recordIdx >= this.maxNumDocs2Ingest) {
            return false;
        }
        if (fileType.equals(CSV)) {
            boolean ok = this.csvRecordsIter.hasNext();
            if (this.csvRecordsIter.isFirstRecOfNewFile() && ignoreLines > 0) {
                List<String> headerList = this.csvRecordsIter.next();
                String[] headerCols = new String[headerList.size()];
                for (int i = 0; i < headerList.size(); i++) {
                    headerCols[i] = headerList.get(i);
                }
                String[] addedFields4CSV = csvRecordsIter.getAddedFieldsForCurrentSourceRec();
                HeaderInformation hi = new HeaderInformation(Arrays.asList(addedFields4CSV), headerCols);
                this.hiMap.put(hi.getKey(), hi);
                ok = this.csvRecordsIter.hasNext();

            }
            return ok;
        } else if (fileType.equals(XML)) {
            return this.xmlFileIterator.hasNext();
        }
        return false;
    }

    public static class HeaderInformation {
        List<String> keyColValues;
        String[] headerCols;
        String key;

        public HeaderInformation(List<String> keyColValues, String[] headerCols) {
            this.keyColValues = keyColValues;
            this.headerCols = headerCols;
            StringBuilder sb = new StringBuilder(keyColValues.size() * 20);
            for (Iterator<String> it = keyColValues.iterator(); it.hasNext(); ) {
                sb.append(it.next());
                if (it.hasNext()) {
                    sb.append(':');
                }
            }
            this.key = sb.toString();
        }

        public List<String> getKeyColValues() {
            return keyColValues;
        }

        public String[] getHeaderCols() {
            return headerCols;
        }

        public String getKey() {
            return key;
        }


        public static String prepKey(String[] keyColValues) {
            StringBuilder sb = new StringBuilder(40);
            for (int i = 0; i < keyColValues.length - 1; i++) {
                sb.append(keyColValues[i]).append(':');
            }
            sb.append(keyColValues[keyColValues.length - 1]);
            return sb.toString();
        }
    }
}
