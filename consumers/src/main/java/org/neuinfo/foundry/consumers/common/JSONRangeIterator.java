package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 4/10/17.
 */
public class JSONRangeIterator implements Iterator<JSONObject> {
    int lowerBound;
    int upperBound;
    int currentIdx = 0;
    Iterator<String> idIterator;

    public JSONRangeIterator(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        currentIdx = lowerBound;
    }

    public JSONRangeIterator(String idLookupFile) throws IOException {

        List<String> idList = new LinkedList<String>();
        BufferedReader in = null;
        try {
            in = Utils.newUTF8CharSetReader(idLookupFile);
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    idList.add(line);
                }
            }
            this.idIterator = idList.iterator();
        } finally {
            Utils.close(in);
        }
    }

    @Override
    public boolean hasNext() {
        if (idIterator != null) {
            return idIterator.hasNext();
        }
        return currentIdx < upperBound;
    }

    @Override
    public JSONObject next() {
        JSONObject json = new JSONObject();
        if (idIterator != null) {
            json.put("value", idIterator.next());
        } else {
            json.put("value", String.valueOf(currentIdx));
            currentIdx++;
        }
        return json;
    }

    @Override
    public void remove() {

    }
}
