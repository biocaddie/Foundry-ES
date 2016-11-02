package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
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
    static Logger logger = Logger.getLogger(JSONFileIterator.class);
    Iterator<JSONObject> jsonObjectIterator;

    public JSONFileIterator(RemoteFileIterator jsonFileIterator, String docElement) throws Exception {
        this.jsonFileIterator = jsonFileIterator;
        this.docElement = docElement;
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
            JSONArray jsArr =  new JSONArray(jsonStr);
            jsList = new ArrayList<JSONObject>(jsArr.length());
            for(int i = 0; i < jsArr.length(); i++) {
                JSONObject el = jsArr.getJSONObject(i);
                jsList.add(el);
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
                            jsList.add((JSONObject) oe);
                        }
                    }
                } else if (o instanceof JSONObject) {
                    jsList.add((JSONObject) o);
                }
            }
        }
        this.jsonObjectIterator = jsList.iterator();
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
                    prepHandler();
                    return jsonObjectIterator.hasNext();
                } else {
                    return false;
                }
            }
        } catch(Throwable t) {
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
}
