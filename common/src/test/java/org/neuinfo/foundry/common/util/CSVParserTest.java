package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileReader;
import java.util.List;

/**
 * Created by bozyurt on 8/5/15.
 */
public class CSVParserTest extends TestCase {
    public CSVParserTest(String name) {
        super(name);
    }

    public void testParsing() throws Exception {
        String[] headerCols;
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader("/var/burak/foundry/test_data/consumer_csv_5664067546818111557.csv"));
            CSVParser parser = new CSVParser();
            List<String> headerList = parser.extractColumns(in);
            headerCols = new String[headerList.size()];
            for (int i = 0; i < headerList.size(); i++) {
                headerCols[i] = headerList.get(i);
            }
            while (true) {
                try {
                    List<String> row = parser.extractColumns(in);
                    if (row.size() <= 8) {
                        for (int i = 0; i < headerCols.length; i++) {
                            String colValue = row.size() > i ? row.get(i) : "";
                            System.out.println(headerCols[i] + ": " + colValue);
                        }
                        System.out.println("--------------------");
                    }
                } catch (EOFException x) {
                    break;
                }
            }

        } finally {
            Utils.close(in);
        }

    }
}
