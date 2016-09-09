package org.neuinfo.foundry.common.util;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 8/3/15.
 */
public class CSVParser {
    protected List<List<String>> rows = new LinkedList<List<String>>();
    protected String delimiter;

    public CSVParser() {
        this(",");
    }

    public CSVParser(String delimiter) {
        this.delimiter = delimiter;
    }


    /**
     * Given a CSV file, parses each line to columns and stores the result in the
     * property rows to be retrieved.
     *
     * @param filename the CSV file to be parsed
     * @throws IOException
     * @see #getRows
     */
    public void extractData(String filename) throws IOException {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
            try {
                List<String> row = extractColumns(in);
                rows.add(row);
            } catch (EOFException ex) {
            }

        } finally {
            Utils.close(in);
        }
    }

    /**
     * @return a list of <code>List</code> objects holding the column values
     * extracted as strings
     */
    public List<List<String>> getRows() {
        return rows;
    }

    /**
     * Extracts columns from a line of input from the data source and returns a
     * list of <code>String</code> objects corresponding to the columns extracted
     * for the parsed line.
     *
     * @param in a Reader to get the input for the CSV format data source
     * @return a list of <code>String</code> objects corresponding to the columns
     * extracted for the parsed record
     * @throws IOException
     */
    public List<String> extractColumns(BufferedReader in) throws IOException {
        List<String> cols = new LinkedList<String>();
        char[] sepArr = {','};
        char[] recSep = {'\n'};
        if (!this.delimiter.equals(",")) {
            sepArr = this.delimiter.toCharArray();
        }
        while (!checkForSeparator(in, recSep)) {
            StringBuilder buf = extractField(in, sepArr, false);
            // System.out.println(buf.toString());
            cols.add(buf.toString());
        }
        return cols;
    }

    private boolean checkForSeparator(BufferedReader in, char[] sepArr)
            throws IOException {
        int c = -1;
        in.mark(sepArr.length + 1);
        c = in.read();
        // System.out.println((char) c);
        if (c == sepArr[0]) {
            // in.mark( sepArr.length );
            boolean sepFound = true;
            for (int i = 1; i < sepArr.length; ++i) {
                c = in.read();
                if (c == -1) {
                    throw new EOFException();
                } else if (c != sepArr[i]) {
                    in.reset();
                    return false;
                }
            }
            if (sepFound)
                return true;
        } else {
            in.reset();
            // System.out.println("checkForSeparator c="+ (char) c);
        }
        return false;
    }

    private StringBuilder extractField(BufferedReader in, char[] sepArr,
                                       boolean ignoreSpace) throws IOException {
        StringBuilder buf = new StringBuilder();

        boolean inString = false;
        boolean useCSV = false;
        boolean finished = false;
        boolean first = true;
        int lastChar = -1;
        while (!finished) {
            in.mark(sepArr.length);
            int c = getChar(in);
            if (c == -1)
                throw new EOFException();
            if (c == '\n' || c == '\r') {
                // encountered an eol
                if (!inString) {
                    in.reset();
                    break;
                }
            }

            if (first && ignoreSpace) {
                if (c == ' ' || c == '\t')
                    continue;
                else if (c == '"' && sepArr[0] == ',') {
                    inString = true;
                    useCSV = true;
                }
            } else {
                if (first && c == '"' && sepArr[0] == ',') {
                    useCSV = true;
                    inString = true;
                    first = false;
                    continue;
                }
                first = false;
            }
            if (c == sepArr[0]) {
                // test
                if (useCSV && lastChar != '"') {
                    buf.append((char) c);
                    lastChar = c;
                    continue;
                } else if (useCSV && inString) {
                    buf.append((char) c);
                    lastChar = c;
                    continue;
                }

                in.mark(sepArr.length);
                boolean sepFound = true;
                for (int i = 1; i < sepArr.length; ++i) {
                    c = in.read();
                    if (c == -1) {
                        finished = true;
                        throw new EOFException();
                    } else if (c != sepArr[i]) {
                        in.reset();
                        sepFound = false;
                        break;
                    }
                }
                if (sepFound)
                    break; // number delimiter found so stop parsing
            } else if (useCSV && c == '"') {
                int prevChar = lastChar;

                lastChar = c;
                in.mark(1);
                c = in.read();
                if (c == -1) {
                    throw new EOFException();
                } else if (c == '"' && prevChar == ',') {
                    buf.append((char) c);
                    // for cases like "", <some text>"
                    lastChar = -1;
                    inString = false;
                } else if (c == '"' && inString) {
                    // quote escaping, skip the second quote
                    buf.append((char) c);
                } else {
                    in.reset();
                    inString = false;
                }
            } else {
                buf.append((char) c);
                lastChar = c;
                first = false;
            }
        }
        return buf;
    }

    protected int getChar(BufferedReader in) throws IOException {
        int c = in.read();
        return c;
    }


    public static void main(String[] args) throws IOException {
        String text = "20020,Sorafenib KINOMEscan,KINOMEscan,\"The KINOMEscan assay platform is based on a competition binding assay that is run for a compound of interest agains     t each of a panel of 317 to 456 kinases. The assay has three components: a kinase-tagged phage, a test compound, and an immobilized ligand that the compound competes with to displace the kinase. The amount of kinase bound to the immobilized ligand is determined using quantitative PCR of the DNA tag.  Results for each kinase are reported as \"\"Percent of control\"\", where the control is DMSO and where a 100% result means no inhibition of kinase binding to the ligand in the presence of the compound, and where low percent results mean strong inhibition. The KINOMEscan data are presented graphically on TREEspot Kin     ase Dendrograms (http://www.kinomescan.com/Tools---Resources/Study-Reports---     Data-Analysis).  For this study, HMS LINCS investigators have graphed results for kinases classified as 35 \"\"percent of control\"\" (in the presence of the compound, the kinase is 35% as active for binding ligand in the presence of DMSO), 5 \"\"percent of control\"\" and 1 \"\"percent of control\"\".\n";
        String csvLine = "20020,\"This is a \"\"test\"\". This is another \"\"test\"\".\",5\n";
        csvLine = text;
        csvLine = "10041-101,BI-2536,NPK33-1-98-1,6,C03,,5637,11.11,1,178948,695,543,142,,10,78.13,20.43,1.44\n";
        CSVParser parser = new CSVParser();
        List<String> rows = parser.extractColumns(new BufferedReader(new StringReader(csvLine)));
        for (String row : rows) {
            System.out.println(row);
        }
    }
}