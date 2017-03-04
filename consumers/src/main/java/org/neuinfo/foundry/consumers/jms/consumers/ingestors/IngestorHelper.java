package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import com.mongodb.BasicDBObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.common.ServiceFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 8/3/15.
 */
public class IngestorHelper {
    static Pattern templateVarPattern = Pattern.compile("\\$\\{(\\w[\\w\\s\\.]+)\\}");

    public static boolean isDocumentServiceURL(String srcURL) {
        return srcURL.startsWith("ds:");
    }


    public static Map<String, List<String>> getSourceData(String srcURL, String collectionName) throws Exception {
        DocumentIngestionService dis = null;
        Map<String, List<String>> map = new LinkedHashMap<String, List<String>>();
        try {
            dis = ServiceFactory.getInstance().createDocumentIngestionService();
            DataSourceURL dsu = new DataSourceURL(srcURL);
            List<String> fields = new ArrayList<String>(dsu.getColNames().size());
            for(String colName : dsu.getColNames()) {
                fields.add("OriginalDoc." + colName);
            }

            List<BasicDBObject> documents = dis.findDocuments(dsu.getSrcId(), dsu.getDataSource(),
                    collectionName, fields);
            for(BasicDBObject dbo: documents) {
                System.out.println(dbo);
            }
            for (String colName : dsu.getColNames()) {
                List<String> list = new ArrayList<String>(documents.size());
                map.put(colName, list);
            }
            for (BasicDBObject dbo : documents) {
                Iterator<String> iter = fields.iterator();
                for (String colName : dsu.getColNames()) {
                    String fieldName = iter.next();
                    List<String> list = map.get(colName);
                    String value = getValue(dbo, fieldName);
                    list.add(value);
                }
            }
        } finally {
            if (dis != null) {
                dis.shutdown();
            }
        }
        return map;
    }

    public static String getValue(BasicDBObject bdo, String colName) {
        int idx = colName.indexOf('.');
        if (idx == -1) {
            return bdo.get(colName).toString();
        }
        String[] toks = colName.split("\\.");
        BasicDBObject p = bdo;
        int i = 0;
        while (p != null && i < toks.length - 1) {
            p = (BasicDBObject) p.get(toks[i]);
            i++;
        }
        if (p == null) {
            return null;
        }
        String lastTok = toks[toks.length - 1];
        Object value = p.get(lastTok);
        return value.toString();
    }


    public static List<String> extractTemplateVariables(String url) {
        Matcher matcher = templateVarPattern.matcher(url);
        if (matcher.find()) {
            matcher.reset();
            List<String> templateVarList = new LinkedList<String>();
            while (matcher.find()) {
                templateVarList.add(matcher.group(1));
            }
            return templateVarList;
        } else {
            return null;
        }
    }

    public static String createURL(String urlTemplate, Map<String, String> templateVar2ValueMap) {
        Matcher matcher = templateVarPattern.matcher(urlTemplate);
        StringBuilder sb = new StringBuilder(urlTemplate.length());
        int offset = 0;
        while (matcher.find()) {
            String templateVar = matcher.group(1);
            String value = templateVar2ValueMap.get(templateVar);
            sb.append(urlTemplate.substring(offset, matcher.start()));
            sb.append(value);
            offset = matcher.end();
        }
        if (offset < urlTemplate.length()) {
            sb.append(urlTemplate.substring(offset));
        }
        return sb.toString();
    }

    public static class DataSourceURL {
        final String srcId;
        final String dataSource;
        final List<String> colNames;

        public DataSourceURL(String srcURL) {
            String[] toks = srcURL.split(":");
            Assertion.assertTrue(toks[0].equals("ds"));
            Assertion.assertTrue(toks.length == 4);
            this.srcId = toks[1];
            if (toks[2].isEmpty()) {
                this.dataSource = null;
            } else {
                this.dataSource = toks[2];
            }
            colNames = Arrays.asList(toks[3].split(","));
        }

        public String getSrcId() {
            return srcId;
        }

        public String getDataSource() {
            return dataSource;
        }

        public List<String> getColNames() {
            return colNames;
        }
    }

}
