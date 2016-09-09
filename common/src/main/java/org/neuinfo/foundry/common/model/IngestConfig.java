package org.neuinfo.foundry.common.model;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 8/25/15.
 */
public class IngestConfig {
    String name;
    List<Param> params = new LinkedList<Param>();

    public IngestConfig(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<Param> getParams() {
        return params;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("name", name);
        JSONArray jsArr = new JSONArray();
        json.put("params", jsArr);
        for (Param param : params) {
            jsArr.put(param.toJSON());
        }
        return json;
    }

    public static IngestConfig fromJSON(JSONObject json) {
        String name = json.getString("name");
        IngestConfig ic = new IngestConfig(name);

        JSONArray jsonArray = json.getJSONArray("params");
        for (int i = 0; i < jsonArray.length(); i++) {
            ic.params.add(Param.fromJSON(jsonArray.getJSONObject(i)));
        }
        return ic;
    }

    public static class Param {
        String name;
        String description;
        boolean required = true;
        String defaultValue = null;
        String fileType = null;
        String[] choices = null;

        public Param(String name, String description, boolean required,
                     String defaultValue, String fileType, String[] choices) {
            this.name = name;
            this.description = description;
            this.required = required;
            this.defaultValue = defaultValue;
            this.fileType = fileType;
            this.choices = choices;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public boolean isRequired() {
            return required;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getFileType() {
            return fileType;
        }

        public String[] getChoices() {
            return choices;
        }

        public JSONObject toJSON() {
            JSONObject json = new JSONObject();
            json.put("name", name);
            json.put("desc", description);
            json.put("required", required);
            if (defaultValue != null) {
                json.put("default", defaultValue);
            }
            if (fileType != null) {
                json.put("fileType", fileType);
            }
            if (choices != null) {
                JSONArray jsArr = new JSONArray();
                for (int i = 0; i < choices.length; i++) {
                    jsArr.put(choices[i]);
                }
                json.put("choices", jsArr);
            }
            return json;
        }

        public static Param fromJSON(JSONObject json) {
            String name = json.getString("name");
            String desc = json.getString("desc");
            boolean required = json.getBoolean("required");
            String defaultValue = null;
            if (json.has("default")) {
                defaultValue = json.getString("default");
            }
            String fileType = null;
            if (json.has("fileType")) {
                fileType = json.getString("fileType");
            }
            String[] choices = null;
            if (json.has("choices")) {
                JSONArray jsArr = json.getJSONArray("choices");
                choices = new String[jsArr.length()];
                for (int i = 0; i < jsArr.length(); i++) {
                    choices[i] = jsArr.getString(i);
                }
            }
            return new Param(name, desc, required, defaultValue, fileType, choices);
        }
    }
}
