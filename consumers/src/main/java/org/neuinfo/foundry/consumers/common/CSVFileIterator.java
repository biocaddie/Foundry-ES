package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.util.CSVParser;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;

import java.io.*;
import java.util.Iterator;
import java.util.List;

/**
 * Streaming CSV multiple file iterator for minimum memory footprint for very large data files
 * <p/>
 * Created by bozyurt on 10/7/15.
 */
public class CSVFileIterator implements Iterator<List<String>> {
    RemoteFileIterator fileIterator;
    CSVParser csvParser;
    List<String> currentRow;
    BufferedReader in;
    File curCSVFile;
    boolean firstRecOfNewFile = false;


    public CSVFileIterator(RemoteFileIterator fileIterator, String delimiter) throws Exception {
        this.fileIterator = fileIterator;
        if (this.fileIterator.hasNext()) {
            curCSVFile = this.fileIterator.next();
            this.csvParser = new CSVParser(delimiter);
            in = new BufferedReader(new FileReader(curCSVFile));
            this.firstRecOfNewFile = true;
        }
    }


    private void prepareNextFile() {
        Utils.close(in);
        this.currentRow = null;
        if (!this.fileIterator.hasNext()) {
            return;
        }
        curCSVFile = fileIterator.next();
        try {
            in = new BufferedReader(new FileReader(curCSVFile));
            this.currentRow = csvParser.extractColumns(in);
        } catch (EOFException x) {
            Utils.close(in);
            prepareNextFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if (in == null) {
            return false;
        }
        try {
            this.currentRow = csvParser.extractColumns(in);
            return this.currentRow != null;
        } catch (EOFException x) {
            prepareNextFile();
            this.firstRecOfNewFile = true;
            return this.currentRow != null;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public List<String> next() {
        this.firstRecOfNewFile = false;
        return this.currentRow;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean isFirstRecOfNewFile() {
        return firstRecOfNewFile;
    }
    public String[] getAddedFieldsForCurrentSourceRec() {
        return fileIterator.getAddedFieldsForCurrentSourceRec();
    }
}
