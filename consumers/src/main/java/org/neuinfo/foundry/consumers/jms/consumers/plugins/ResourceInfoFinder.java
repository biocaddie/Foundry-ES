package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import org.apache.commons.collections.map.LRUMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Parameters;

import java.sql.*;
import java.util.*;

/**
 * Created by bozyurt on 6/19/15.
 * <p/>
 * PDB nif-0000-00135 biocaddie-pdb-0001
 */
public class ResourceInfoFinder {
    static LRUMap cache = new LRUMap(50);


    public static ResourceInfo getResourceInfo(String nifId) throws Exception {
        ResourceInfo ri = (ResourceInfo) cache.get(nifId);
        if (ri == null) {
            Map<String, Object> rdm = getResourcesFromDISCO(nifId);
            String name = getValue(rdm.get("resource_name"));
            String url = getValue(rdm.get("url"));
            String description = getValue(rdm.get("description"));
            String synonyms = getValue(rdm.get("synonym"));
            String keywordStr = getValue(rdm.get("keyword"));
            String acronymStr = getValue(rdm.get("acronym"));

            ri = new ResourceInfo(name, description, url);
            if (keywordStr != null) {
                String[] toks = keywordStr.split("\\s*,\\s*");
                for (String tok : toks) {
                    ri.addKeyword(tok);
                }
            }
            if (synonyms != null) {
                String[] toks = synonyms.split("\\s*,\\s*");
                for (String tok : toks) {
                    ri.addAltName(tok);
                }
            }
            if (acronymStr != null) {
                String[] toks = acronymStr.split("\\s*,\\s*");
                for (String tok : toks) {
                    ri.addAcronym(tok);
                }
            }

            cache.put(nifId, ri);
        }
        return ri;
    }

    public static String getValue(Object value) {
        return value == null ? null : value.toString();
    }

    static Map<String, Object> getResourcesFromDISCO(String nifId) throws Exception {
        Connection con = null;
        try {
            Parameters parameters = Parameters.getInstance();
            // required for maven assembly plugin to 'discover and include' JDBC driver (IBO)
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.put("user", parameters.getParam("disco.user"));
            props.put("password", parameters.getParam("disco.password"));

            con = DriverManager.getConnection(parameters.getParam("disco.dbURL"), props);

            Statement st = con.createStatement();
            st.execute("set search_path to 'dvp'");
            st.close();
            Map<String, String> rMap = new HashMap<String, String>();
            st = con.createStatement();
            ResultSet rs = st.executeQuery("select * from pr_nlx_144509_1 where see_full_record ='" + nifId + "'");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String colName = metaData.getColumnName(i);
                String colType = metaData.getColumnTypeName(i);
                rMap.put(colName, colType);
            }
            Map<String, Object> rdm = new HashMap<String, Object>();
            if (rs.next()) {
                for (String colName : rMap.keySet()) {
                    rdm.put(colName, rs.getObject(colName));
                }
            }
            rs.close();
            st.close();
            return rdm;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    public static class ResourceInfo {
        String name;
        List<String> acronyms = new LinkedList<String>();
        List<String> altNames = new LinkedList<String>();
        String description;
        String url;
        List<String> keywords = new LinkedList<String>();

        public ResourceInfo(String name, String description, String url) {
            this.name = name;
            this.description = description;
            this.url = url;
        }

        public void addKeyword(String keyword) {
            keywords.add(keyword);
        }

        public void addAltName(String altName) {
            altNames.add(altName);
        }

        public void addAcronym(String acronym) {
            acronyms.add(acronym);
        }

        public String getName() {
            return name;
        }


        public String getDescription() {
            return description;
        }

        public String getUrl() {
            return url;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public List<String> getAcronyms() {
            return acronyms;
        }

        public List<String> getAltNames() {
            return altNames;
        }

        public JSONObject toJSON() {
            JSONObject js = new JSONObject();
            js.put("name", name);
            js.put("url", url);
            if (description != null) {
                js.put("description", description);
            } else {
                js.put("description", description);
            }
            JSONArray jsArr = new JSONArray();
            js.put("altNames", jsArr);
            for (String altName : this.altNames) {
                jsArr.put(altName);
            }
            jsArr = new JSONArray();
            js.put("keywords", jsArr);
            for (String keyword : keywords) {
                jsArr.put(keyword);
            }
            jsArr = new JSONArray();
            js.put("acronyms", jsArr);
            for (String acronym : acronyms) {
                jsArr.put(acronym);
            }
            return js;
        }

    }

    public static void main(String[] args) throws Exception {
        ResourceInfo ri = ResourceInfoFinder.getResourceInfo("nif-0000-00135");
        System.out.println(ri.getName());
    }

}
