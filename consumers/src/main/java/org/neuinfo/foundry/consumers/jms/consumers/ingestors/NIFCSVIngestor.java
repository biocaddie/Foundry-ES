package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.HtmlEntityUtil;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.CSVFileIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.*;
import java.util.*;

/**
 * Created by bozyurt on 11/3/14.
 */
public class NIFCSVIngestor implements Ingestor {
    boolean allowDuplicates = false;
    Map<String, String> optionMap;
    String ingestURL;
    boolean keepMissing;
    int ignoreLines;
    int headerLine;
    String delimiter;
    String textQuote;
    String escapeChar;
    String localeStr;
    boolean sampleMode = false;
    int sampleSize = 1;
    CSVFileIterator csvFileIterator;
    String[] headerCols;
    List<String> currentRow;
    File csvFile;
    int recordIdx = 0;
    int maxNumDocs2Ingest = -1;
    boolean useCache = true;
    static boolean TEST_MODE = false;
    private final static Logger log = Logger.getLogger(NIFCSVIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.allowDuplicates = options.containsKey("allowDuplicates") ?
                Boolean.parseBoolean(options.get("allowDuplicates")) : false;
        this.optionMap = options;
        this.ingestURL = options.get("ingestURL");
        this.keepMissing = options.containsKey("keepMissing") ?
                Boolean.parseBoolean(options.get("keepMissing")) : false;
        this.ignoreLines = Utils.getIntValue(options.get("ignoreLines"), -1); // one based
        this.headerLine = Utils.getIntValue(options.get("headerLine"), -1); // one based
        this.delimiter = options.get("delimiter");
        this.textQuote = options.get("textQuote");
        this.escapeChar = options.get("escapeCharacter");
        this.localeStr = options.get("locale");


        if (HtmlEntityUtil.isHtmlEntity(this.textQuote)) {
            this.textQuote = HtmlEntityUtil.getChar(this.textQuote).toString();
        }
        if (HtmlEntityUtil.isHtmlEntity(this.escapeChar)) {
            this.escapeChar = HtmlEntityUtil.getChar(this.escapeChar).toString();
        }
        if (HtmlEntityUtil.isHtmlEntity(this.delimiter)) {
            this.delimiter = HtmlEntityUtil.getChar(this.delimiter).toString();
        }
        if (options.containsKey("maxDocs")) {
            this.maxNumDocs2Ingest = Utils.getIntValue(options.get("maxDocs"), -1);
        }
        if (options.containsKey("testMode")) {
            TEST_MODE = Boolean.parseBoolean(options.get("testMode"));
        }

        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        this.useCache = options.containsKey("useCache") ? Boolean.parseBoolean(options.get("useCache")) : true;
    }

    @Override
    public void startup() throws Exception {
        if (TEST_MODE) {
            this.csvFile = new File(System.getProperty("user.home") + "/bloomington.csv");
        } else {
            this.csvFile = ContentLoader.getContent(this.ingestURL, null, useCache);
        }
        Assertion.assertNotNull(csvFile);
        this.csvFileIterator = new CSVFileIterator(new RemoteFileIterator(Arrays.asList(csvFile)),
                this.delimiter);
        Assertion.assertNotNull(this.headerLine);
        if (ignoreLines > 0 && csvFileIterator.hasNext()) {
            List<String> headerList = csvFileIterator.next();
            for (Iterator<String> it = headerList.iterator(); it.hasNext(); ) {
                String header = it.next();
                if (header.trim().length() == 0) {
                    it.remove();
                }
            }
            this.headerCols = new String[headerList.size()];
            for (int i = 0; i < headerList.size(); i++) {
                this.headerCols[i] = headerList.get(i);
            }
        }

    }

    /**
     * assumption: <code>hasNext()</code> is called first
     *
     * @return
     */
    @Override
    public Result prepPayload() {
        this.currentRow = this.csvFileIterator.next();
        this.recordIdx++;
        int len;
        if (this.currentRow != null) {
            JSONObject json = new JSONObject();
            len = this.currentRow.size();
            for (int i = 0; i < this.headerCols.length; i++) {
                if (len <= i) {
                    json.put(headerCols[i], "");
                    // System.out.println("More record fields than header fields at recordIdx:" + recordIdx);
                } else {
                    String value = this.currentRow.get(i);
                    json.put(headerCols[i], value);
                }
            }
            return new Result(json, Result.Status.OK_WITH_CHANGE);
        }
        return null;
    }

    @Override
    public String getName() {
        return "NIFCSVIngestor";
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
        if (!TEST_MODE) {
            if (csvFile != null) {
                //  System.out.println("deleting CSV file:" + csvFile);
                // csvFile.delete();
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.maxNumDocs2Ingest > 0 && this.recordIdx >= this.maxNumDocs2Ingest) {
            return false;
        }
        boolean ok = this.csvFileIterator.hasNext();
        return ok;
    }

    /*
    File getCSVContent2() throws Exception {
        boolean gzippedFile = ingestURL.endsWith(".gz");
        if (ingestURL.startsWith("file://")) {
            String filePath = ingestURL.replaceFirst("file://", "");
            String cacheFileName = filePath;
            cacheFileName = cacheFileName.replaceAll("[\\./\\(\\)]", "_");
            File cacheFile = new File(Constants.CACHE_ROOT, cacheFileName);
            if (cacheFile.isFile()) {
                return cacheFile;
            }
            if (gzippedFile) {
                BufferedInputStream in = null;
                BufferedOutputStream out = null;
                try {
                    in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(new File(filePath))));
                    out = new BufferedOutputStream(new FileOutputStream(cacheFile));
                    byte[] buffer = new byte[4096];
                    int readBytes;
                    while ((readBytes = in.read(buffer)) != -1) {
                        out.write(buffer, 0, readBytes);
                    }
                    return cacheFile;
                } finally {
                    Utils.close(in);
                    Utils.close(out);
                }
            } else {
                BufferedWriter out = null;
                BufferedReader in = null;
                try {
                    in = new BufferedReader(new FileReader( new File(filePath)));
                    out = Utils.newUTF8CharSetWriter(cacheFile.getAbsolutePath());
                    String line;
                    while ((line = in.readLine()) != null) {
                        out.write(line);
                        out.newLine();
                    }
                    return cacheFile;
                } finally {
                    Utils.close(in);
                    Utils.close(out);
                }
            }
        }
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(ingestURL);
        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);

        try {
            HttpResponse response = client.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String cacheFileName = Utils.fromURL2FileName(ingestURL);
                File cacheFile = new File(Constants.CACHE_ROOT, cacheFileName);
                if (cacheFile.isFile()) {
                    return cacheFile;
                }
                if (gzippedFile) {
                    BufferedInputStream in = null;
                    BufferedOutputStream out = null;
                    try {
                        in = new BufferedInputStream(new GZIPInputStream(entity.getContent()));
                        out = new BufferedOutputStream(new FileOutputStream(cacheFile));
                        byte[] buffer = new byte[4096];
                        int readBytes = 0;
                        while ((readBytes = in.read(buffer)) != -1) {
                            out.write(buffer, 0, readBytes);
                        }
                        return cacheFile;
                    } finally {
                        Utils.close(in);
                        Utils.close(out);
                    }
                } else {
                    BufferedWriter out = null;
                    BufferedReader in = null;
                    try {
                        in = new BufferedReader(new InputStreamReader(entity.getContent()));
                        out = Utils.newUTF8CharSetWriter(cacheFile.getAbsolutePath());
                        String line;
                        while ((line = in.readLine()) != null) {
                            out.write(line);
                            out.newLine();
                        }
                        return cacheFile;
                    } finally {
                        Utils.close(in);
                        Utils.close(out);
                    }
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return null;
    }
    */

}
