package org.neuinfo.foundry.common.config;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by bozyurt on 5/27/14.
 */
public class QueueInfo {
    private final String name;
    private Set<String> headerFieldSet;

    public QueueInfo(String name) {
        this.name = name;
    }

    public void addHeaderField(String headerField) {
        if (headerFieldSet == null) {
            headerFieldSet = new HashSet<String>(7);
        }
        headerFieldSet.add(headerField);
    }

    public boolean hasHeaderFields() {
        return headerFieldSet != null && !headerFieldSet.isEmpty();
    }

    public String getName() {
        return name;
    }

    public Set<String> getHeaderFieldSet() {
        return headerFieldSet;
    }

    public static QueueInfo fromXml(Element elem) throws Exception {
        String name = elem.getAttributeValue("name");
        QueueInfo qi = new QueueInfo(name);
        String text = elem.getAttributeValue("headerFields");
        String[] toks = text.split("\\s*,\\s*");
        for (String tok : toks) {
            qi.addHeaderField(tok);
        }
        return qi;
    }

    public static QueueInfo fromJSON(JSONObject json) throws Exception {
        String name = json.getString("name");
        QueueInfo qi = new QueueInfo(name);
        JSONArray headerFields = json.getJSONArray("headerFields");
        for(int i = 0; i < headerFields.length(); i++) {
            qi.addHeaderField( headerFields.getString(i));
        }
        return qi;
    }

    public JSONObject toJSON() throws Exception {
        JSONObject json = new JSONObject();
        json.put("name", name);
        JSONArray jsArr = new JSONArray();
        for(String headerField : headerFieldSet) {
            jsArr.put(headerField);
        }
        json.put("headerFields", jsArr);
        return json;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueueInfo{");
        sb.append("name='").append(name).append('\'');
        if (headerFieldSet != null) {
            sb.append(", headerFieldSet=").append(headerFieldSet);
        }
        sb.append('}');
        return sb.toString();
    }
}
