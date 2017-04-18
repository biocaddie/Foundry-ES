package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;
import org.neuinfo.foundry.consumers.plugin.IngestorIterable;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 2/27/17.
 */
public class CSVCursor implements IngestorIterable, Joinable {
    int headerLine;
    int ignoreLines;
    String delimiter;
    File csvFile;
    CSVFileIterator csvFileIterator;
    String[] headerCols;
    String resetJsonPath;
    String alias;
    String ignoreValue;
    private String joinValueJsonPath;
    private JSONPathProcessor.Path path;
    List<JSONObject> records;
    JSONObject curRecord;
    Iterator<JSONObject> iterator;
    Map<String, List<JSONObject>> index = null;

    public CSVCursor(String alias, File csvFile) {
        this.alias = alias;
        this.csvFile = csvFile;
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.ignoreLines = Utils.getIntValue(options.get("ignoreLines"), 1); // one based
        this.headerLine = Utils.getIntValue(options.get("headerLine"), 1); // one based
        this.delimiter = options.get("delimiter");
        if (this.delimiter != null && this.delimiter.equals("<TAB>")) {
            this.delimiter = "\t";
        }
        this.ignoreValue = options.get("ignoreValue");
    }

    @Override
    public void startup() throws Exception {
        this.csvFileIterator = new CSVFileIterator(new RemoteFileIterator(Arrays.asList(csvFile)),
                this.delimiter);
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
        this.records = new ArrayList<JSONObject>();
        int count = 0;
        while (csvFileIterator.hasNext()) {
            List<String> row = this.csvFileIterator.next();

            if (row != null) {
                JSONObject json = new JSONObject();
                int len = row.size();
                for (int i = 0; i < this.headerCols.length; i++) {
                    if (len <= i) {
                        json.put(headerCols[i], "");
                    } else {
                        String value = row.get(i);
                        if (ignoreValue == null || !value.equals(ignoreValue)) {
                            json.put(headerCols[i], value);
                        }
                    }
                }
                this.records.add(json);
                ++count;
                System.out.println("processed record " + count);
            }
        }
        this.iterator = this.records.iterator();
    }

    @Override
    public Result prepPayload() {
        JSONObject json = this.iterator.next();
        return new Result(json, Result.Status.OK_WITH_CHANGE);
    }

    @Override
    public boolean hasNext() {
        return this.iterator != null && this.iterator.hasNext();
    }

    @Override
    public void reset(String refValue) throws Exception {
        if (refValue == null) {
            if (this.iterator == null) {
                this.iterator = records.iterator();
            }
        } else {
            if (index == null) {
                index = CursorUtils.prepHashIndex(this.resetJsonPath, this.records);
            }
            List<JSONObject> list = index.get(refValue);
            // List<JSONObject> list = CursorUtils.filter(refValue, this.resetJsonPath, this.records);
            if (list == null) {
                this.iterator = null;
            } else {
                this.iterator = list.iterator();
            }
        }
    }

    @Override
    public JSONObject next() {
        this.curRecord = this.iterator.next();
        return curRecord;
    }

    @Override
    public JSONObject peek() {
        return this.curRecord;
    }

    @Override
    public String getJoinValue() {
        return CursorUtils.extractStringValue(this.curRecord, path);
    }

    @Override
    public String getAlias() {
        return this.alias;
    }

    @Override
    public void setResetJsonPath(String resetJsonPath) {
        this.resetJsonPath = resetJsonPath;
    }

    @Override
    public void setJoinValueJsonPath(String joinValueJsonPath) throws Exception {
        this.joinValueJsonPath = joinValueJsonPath;
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        this.path = processor.compile(joinValueJsonPath);
    }

    @Override
    public List<ColumnMeta> getColumnMetaList() {
        return null;
    }


    public static void main(String[] args) throws Exception {
        Map<String,String> options = new HashMap<String, String>();
        options.put("delimiter","\t");
        options.put("ignoreValue","0.0");
        CSVCursor cursor = new CSVCursor("a", new File("/home/bozyurt/Downloads/neurosynth/features.txt"));
        cursor.initialize(options);
        cursor.startup();


    }
}
