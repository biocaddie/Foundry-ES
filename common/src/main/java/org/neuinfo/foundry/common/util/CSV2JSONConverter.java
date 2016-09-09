package org.neuinfo.foundry.common.util;

import au.com.bytecode.opencsv.CSVReader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.List;

/**
 * Created by bozyurt on 5/29/14.
 */
public class CSV2JSONConverter {
    boolean hasHeader;

    public CSV2JSONConverter(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public JSONObject toJSONFromString(String content) throws IOException {
         return toJSON( new StringReader(content));
    }

    public JSONObject toJSONFromFile(String csvFilePath) throws IOException {
        return toJSON(new BufferedReader(new FileReader(csvFilePath)));
    }

    public JSONObject toJSON(Reader in) throws IOException {
        CSVReader reader = new CSVReader(in);
        final List<String[]> rows = reader.readAll();

        JSONObject wrapper = new JSONObject();
        if (rows.isEmpty()) {
            return wrapper;
        }
        String[] headers = null;
        if (hasHeader) {
            headers = rows.remove(0);
        }
        int noCols = headers != null ? headers.length : rows.get(0).length;
        String[] colNames = new String[noCols];
        if (hasHeader && headers != null) {
            for (int i = 0; i < noCols; i++) {
                String header = headers[i].trim();
                header = header.replaceAll("[\\s-]", "_");
                colNames[i] = header;
            }
        } else {
            for (int i = 0; i < noCols; i++) {
                colNames[i] = "column" + (i + 1);
            }
        }

        JSONArray jsArr = new JSONArray();
        wrapper.put("rows", jsArr);
        for (String[] row : rows) {
            JSONObject rowJSON = new JSONObject();
            Assertion.assertTrue(row.length == noCols);
            for (int i = 0; i < noCols; i++) {
                String colValue = row[i].trim();
                rowJSON.put(colNames[i], colValue);
            }
            jsArr.put(rowJSON);
        }
        return wrapper;
    }

    public static void main(String[] args) throws IOException {
        String csv = Utils.loadAsString("/tmp/urls.csv");
        CSV2JSONConverter converter = new CSV2JSONConverter(true);

        JSONObject json = converter.toJSONFromString(csv);
        System.out.println(json.toString(2));

    }
}
