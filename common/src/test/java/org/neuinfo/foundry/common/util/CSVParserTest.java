package org.neuinfo.foundry.common.util;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by bozyurt on 8/5/15.
 */
public class CSVParserTest {

    @Test
    public void testParsing() throws Exception {
        String[] headerCols;
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(
                    CSVParserTest.class.getClassLoader().getResourceAsStream("testdata/GEO_DPP.csv")));
            CSVParser parser = new CSVParser();
            List<String> headerList = parser.extractColumns(in);
            headerCols = new String[headerList.size()];
            for (int i = 0; i < headerList.size(); i++) {
                headerCols[i] = headerList.get(i);
            }
            assertEquals(headerCols.length, 6);
            String[] expectedHeaders = {"title","accession","pmid","cited_pmid","cites_pmid","cc"};
            assertArrayEquals(expectedHeaders, headerCols);
            int count = 0;
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
                    count++;
                } catch (EOFException x) {
                    break;
                }
            }
            assertEquals(5, count);

        } finally {
            Utils.close(in);
        }

    }
}
