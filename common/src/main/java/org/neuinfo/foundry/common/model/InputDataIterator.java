package org.neuinfo.foundry.common.model;


import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 12/15/15.
 */
public class InputDataIterator implements Iterator<String> {
    List<String> lines = new LinkedList<String>();
    Iterator<String> iter = null;
    String content;
    URL url;

    public InputDataIterator(URL url) {
        this.url = url;
    }

    public InputDataIterator(String content) {
        this.content = content;
    }

    public InputDataIterator(File inputFile) throws IOException {
        BufferedReader bin = null;
        try {
            bin = Utils.newUTF8CharSetReader(inputFile.getAbsolutePath());
            String line;
            while ((line = bin.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    lines.add(line);
                }
            }
        } finally {
            Utils.close(bin);
        }
        iter = lines.iterator();

    }

    @Override
    public boolean hasNext() {
        return iter != null && iter.hasNext();
    }

    @Override
    public String next() {
        return iter.next();
    }

    @Override
    public void remove() {
        // no op
    }

    public String getContent() {
        return content;
    }

    public URL getUrl() {
        return url;
    }

}
