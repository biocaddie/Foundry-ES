package org.neuinfo.foundry.common.util;

import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;
import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.File;

/**
 * Created by bozyurt on 9/17/15.
 */
public class GEOParser implements IDataParser {
    InputDataIterator iter;

    public GEOParser() {
    }



    @Override
    public void initialize(InputDataIterator iterator) {
        this.iter = iterator;
    }

    @Override
    public JSONObject toJSON() throws Exception {
        JSONObject json = new JSONObject();
        while (iter.hasNext()) {
            String line = iter.next();
            if (line.startsWith("!")) {
                int idx = line.indexOf(" = ");
                if (idx != -1) {
                    String name = line.substring(1, idx).trim();
                    String value = line.substring(idx + 3).trim();
                    json.put(name, value);
                }
            }
        }
        return json;
    }
}
