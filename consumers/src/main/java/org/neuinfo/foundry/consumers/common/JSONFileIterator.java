package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 1/29/16.
 */
public class JSONFileIterator implements Iterator<JSONObject> {
    String docElement;
    RemoteFileIterator jsonFileIterator;
    File curFile;
    boolean empty = false;
    Filter filter;
    int nonFilterCount = 0;

    static Logger logger = Logger.getLogger(JSONFileIterator.class);
    Iterator<JSONObject> jsonObjectIterator;

    public JSONFileIterator(RemoteFileIterator jsonFileIterator, String docElement,
                            String filterJsonPath, String filterValue) throws Exception {
        this.jsonFileIterator = jsonFileIterator;
        this.docElement = docElement;
        if (filterJsonPath != null && filterValue != null) {
            filter = new Filter(filterJsonPath, filterValue);
        }
        if (jsonFileIterator.hasNext()) {
            prepHandler();
        } else {
            empty = true;
        }
    }

    void prepHandler() throws Exception {
        this.curFile = jsonFileIterator.next();
        String jsonStr = Utils.loadAsString(this.curFile.getAbsolutePath());
        List<JSONObject> jsList;
        if (docElement == null || docElement.length() == 0) {
            // assumption a json array is at the root of the document
            JSONArray jsArr = new JSONArray(jsonStr);
            jsList = new ArrayList<JSONObject>(jsArr.length());
            for (int i = 0; i < jsArr.length(); i++) {
                JSONObject el = jsArr.getJSONObject(i);
                if (filter == null || filter.satisfied(el)) {
                    jsList.add(el);
                }
            }
            if (filter != null) {
                this.nonFilterCount = jsArr.length();
            }
        } else {
            JSONObject json = new JSONObject(jsonStr);
            JSONPathProcessor processor = new JSONPathProcessor();
            List<Object> list = processor.find("$..'" + docElement + "'", json);
            Assertion.assertTrue(!list.isEmpty());
            jsList = new ArrayList<JSONObject>();
            for (Object o : list) {
                if (o instanceof JSONArray) {
                    JSONArray jsArr = (JSONArray) o;
                    for (int i = 0; i < jsArr.length(); i++) {
                        Object oe = jsArr.get(i);
                        if (oe instanceof JSONObject) {
                            JSONObject el = (JSONObject) oe;
                            if (filter == null || filter.satisfied(el)) {
                                jsList.add(el);
                            }
                        }
                    }
                    if (filter != null) {
                        this.nonFilterCount += jsArr.length();
                    }
                } else if (o instanceof JSONObject) {
                    JSONObject el = (JSONObject) o;
                    if (filter == null || filter.satisfied(el)) {
                        jsList.add(el);
                    }
                    if (filter != null) {
                        this.nonFilterCount++;
                    }
                }
            }
        }
        this.jsonObjectIterator = jsList.iterator();
    }

    public int getNonFilterCount() {
        return nonFilterCount;
    }

    @Override
    public boolean hasNext() {
        if (empty) {
            return false;
        }
        try {
            if (jsonObjectIterator.hasNext()) {
                return true;
            } else {
                if (jsonFileIterator.hasNext()) {
                    do {
                        prepHandler();
                        boolean hasNext = jsonObjectIterator.hasNext();
                        if (hasNext) {
                            return hasNext;
                        }
                    } while(jsonFileIterator.hasNext());
                    return  false;
                } else {
                    return false;
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            return false;
        }
    }

    @Override
    public JSONObject next() {
        return this.jsonObjectIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static class Filter {
        final String filterJsonPath;
        final String filterValue;

        public Filter(String filterJsonPath, String filterValue) {
            this.filterJsonPath = filterJsonPath;
            this.filterValue = filterValue;
        }


        public boolean satisfied(JSONObject json) {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> list;
            try {
                list = processor.find(filterJsonPath, json);
                if (list != null && !list.isEmpty()) {
                    JSONPathProcessor2.JPNode jpNode = list.get(0);
                    return jpNode.getValue().equals(filterValue);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
return false;
        }
    }
}
