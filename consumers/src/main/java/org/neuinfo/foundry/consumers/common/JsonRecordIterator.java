package org.neuinfo.foundry.consumers.common;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by bozyurt on 12/9/16.
 */
public class JsonRecordIterator implements Iterator<JSONObject> {
    JsonParser jsonParser;
    String docElement = null;
    JSONObject curRec;
    boolean first = true;


    public JsonRecordIterator(File jsonFile, String docElement) throws IOException {
        this.docElement = docElement;
        JsonFactory factory = new MappingJsonFactory();
        jsonParser = factory.createParser(jsonFile);
        JsonToken jsonToken = jsonParser.nextToken();
        if (jsonToken == JsonToken.START_ARRAY) {
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                JsonNode node = jsonParser.readValueAsTree();
                this.curRec = new JSONObject(node.toString());
                break;
            }
        } else {
            Assertion.assertTrue(docElement != null && docElement.length() > 0);
            if (jsonToken == JsonToken.START_OBJECT) {
                while ((jsonToken = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
                    String currentName = jsonParser.getCurrentName();
                    if (docElement.equals(currentName)) {
                        jsonToken = jsonParser.nextToken();
                        // System.out.println(jsonToken);
                        if (jsonToken == JsonToken.START_ARRAY) {
                            if (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                                JsonNode node = jsonParser.readValueAsTree();
                                this.curRec = new JSONObject(node.toString());
                                // System.out.println(new JSONObject(node.toString()));
                                break;
                            }
                        }
                    }
                }
            }
        }

    }

    @Override
    public boolean hasNext() {
        if (curRec == null) {
            return false;
        } else if (first) {
            return true;
        } else {
            try {
                this.curRec = null;
                if (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                    JsonNode node = jsonParser.readValueAsTree();
                    this.curRec = new JSONObject(node.toString());
                }
                return curRec != null;
            } catch (Exception x) {
                x.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public JSONObject next() {
        if (first) {
            first = false;
        }
        return curRec;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) throws Exception {
        JsonRecordIterator it = new JsonRecordIterator(new File("/tmp/test.json"), "data");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
