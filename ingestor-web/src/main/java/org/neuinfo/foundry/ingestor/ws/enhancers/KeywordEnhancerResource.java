package org.neuinfo.foundry.ingestor.ws.enhancers;

import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.CinergiXMLUtils;
import org.neuinfo.foundry.common.util.CinergiXMLUtils.KeywordInfo;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathException;
import java.io.StringWriter;
import java.net.URI;
import java.util.*;

/**
 * Created by bozyurt on 1/16/15.
 */
@Path("cinergi/enhancers/keyword")
@Api(value = "cinergi/enhancers/keyword", description = "Keyword enhancement of ISO XML Metadata documents")
public class KeywordEnhancerResource {
    private final static Logger logger = Logger.getLogger(KeywordEnhancerResource.class);
    String serviceURL = "http://tikki.neuinfo.org:9000/scigraph/annotations/entities";

    @POST
    @Consumes({"application/xml"})
    @Produces({"application/xml"})
    @ApiResponses(value = {@ApiResponse(code = 400, message = "no ISO XML document is supplied"),
            @ApiResponse(code = 500, message = "An internal error occurred during keyword enhancement")})
    @ApiOperation(value = "Enhance an ISO Metadata Document with keywords",
            notes = "Uses the abstract and title text in the given metadata document to detect keywords",
            response = String.class)
    public Response post(@ApiParam(value = "ISO Metadata XML document for keyword enhancements", required = true)
                         String isoMetaXml) {
        if (isoMetaXml == null) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
        }
        List<String> jsonPaths = new ArrayList<String>(5);
        jsonPaths.add("$..'gmd:abstract'.'gco:CharacterString'.'_$'");
        jsonPaths.add("$..'gmd:title'.'gco:CharacterString'.'_$'");
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONPathProcessor processor = new JSONPathProcessor();
        Map<String, Keyword> keywordMap = new LinkedHashMap<String, Keyword>();
        try {
            Element docEl = Utils.readXML(isoMetaXml);
            JSONObject json = converter.toJSON(docEl);
            for (String jsonPath : jsonPaths) {
                List<Object> objects = processor.find(jsonPath, json);
                if (objects != null && !objects.isEmpty()) {
                    String text2Annotate = (String) objects.get(0);
                    annotateEntities(jsonPath, text2Annotate, keywordMap);
                }
            }
            JSONArray jsArr = new JSONArray();
            Set<String> allowedCategorySet = new HashSet<String>();
            allowedCategorySet.add("dataCenter");
            allowedCategorySet.add("instrument");
            allowedCategorySet.add("theme");
            allowedCategorySet.add("platform");
            for (Keyword keyword : keywordMap.values()) {
                if (keyword.hasCategory() && keyword.hasAnyCategory(allowedCategorySet)) {
                    JSONObject kwJson = keyword.toJSON();
                    jsArr.put(kwJson);
                    System.out.println(kwJson.toString(2));
                    System.out.println("---------------------------");
                }
            }
            Map<String, List<KeywordInfo>> category2KWIListMap = new HashMap<String, List<KeywordInfo>>(7);
            for (int i = 0; i < jsArr.length(); i++) {
                JSONObject kwJson = jsArr.getJSONObject(i);
                Keyword kw = Keyword.fromJSON(kwJson);
                Set<String> categories = kw.getCategories();
                if (categories.size() == 1) {
                    String category = categories.iterator().next();
                    KeywordInfo kwi = new KeywordInfo(kw.getTerm(), category, null);
                    List<KeywordInfo> kwiList = category2KWIListMap.get(category);
                    if (kwiList == null) {
                        kwiList = new ArrayList<KeywordInfo>(10);
                        category2KWIListMap.put(category, kwiList);
                    }
                    kwiList.add(kwi);
                }

            }
            if (!category2KWIListMap.isEmpty()) {
                for (List<KeywordInfo> kwiList : category2KWIListMap.values()) {
                    filterPlurals(kwiList);
                }
                docEl = CinergiXMLUtils.addKeywords(docEl, category2KWIListMap);
            }
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            StringWriter sw = new StringWriter(isoMetaXml.length());
            xout.output(docEl, sw);
            return Response.ok(sw.toString()).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }


    void filterPlurals(List<KeywordInfo> kwiList) {
        Set<KeywordInfo> plurals = new HashSet<KeywordInfo>();
        for (int i = 0; i < kwiList.size(); i++) {
            KeywordInfo kwiRef = kwiList.get(i);
            for (int j = i + 1; j < kwiList.size(); j++) {
                KeywordInfo kwi2 = kwiList.get(j);
                if (kwiRef.getTerm().startsWith(kwi2.getTerm()) &&
                        kwiRef.getTerm().equals(kwi2.getTerm() + "S")) {
                    plurals.add(kwi2);
                } else if (kwi2.getTerm().startsWith(kwiRef.getTerm()) &&
                        kwi2.getTerm().equals(kwiRef.getTerm() + "S")) {
                    plurals.add(kwiRef);
                }
            }
        }
        if (!plurals.isEmpty()) {
            System.out.println(plurals);
            for (Iterator<KeywordInfo> it = kwiList.iterator(); it.hasNext(); ) {
                KeywordInfo kwi = it.next();
                if (plurals.contains(kwi)) {
                    it.remove();
                }
            }
        }
    }

    void annotateEntities(String contentLocation, String text, Map<String, Keyword> keywordMap) throws Exception {
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(this.serviceURL);
        builder.setParameter("content", text);
        // minLength=4&longestOnly=true&includeAbbrev=false&includeAcronym=false&includeNumbers=false&callback=fn
        builder.setParameter("minLength", "4");
        builder.setParameter("longestOnly", "true");
        builder.setParameter("includeAbbrev", "false");
        builder.setParameter("includeNumbers", "false");

        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);
        // httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("Accept", "application/json");
        try {
            final HttpResponse response = client.execute(httpGet);
            final HttpEntity entity = response.getEntity();
            if (entity != null) {
                String jsonStr = EntityUtils.toString(entity);
                try {
                    System.out.println(new JSONArray(jsonStr).toString(2));
                    System.out.println("================");
                    JSONArray jsArr = new JSONArray(jsonStr);
                    String textLC = text.toLowerCase();
                    for (int i = 0; i < jsArr.length(); i++) {
                        final JSONObject json = jsArr.getJSONObject(i);
                        if (json.has("token")) {
                            JSONObject tokenObj = json.getJSONObject("token");
                            String id = tokenObj.getString("id");
                            final JSONArray terms = tokenObj.getJSONArray("terms");
                            if (terms.length() > 0) {
                                int start = json.getInt("start");
                                int end = json.getInt("end");
                                String term = findMatchingTerm(terms, textLC);
                                if (term != null) {
                                    Keyword keyword = keywordMap.get(term);
                                    if (keyword == null) {
                                        keyword = new Keyword(term);
                                        keywordMap.put(term, keyword);
                                    }
                                    JSONArray categories = tokenObj.getJSONArray("categories");
                                    String category = "";
                                    if (categories.length() > 0) {
                                        category = categories.getString(0);
                                    }
                                    EntityInfo ei = new EntityInfo(contentLocation, id, start, end, category);
                                    keyword.addEntityInfo(ei);
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    logger.error("annotateEntities", t);
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
    }

    public static String findMatchingTerm(JSONArray jsArr, String text) {
        for (int i = 0; i < jsArr.length(); i++) {
            String term = jsArr.getString(i);
            if (text.indexOf(term.toLowerCase()) != -1) {
                return term;
            }
        }
        return null;
    }

    public static class Keyword {
        private String term;
        List<EntityInfo> entityInfos = new LinkedList<EntityInfo>();

        public Keyword(String term) {
            this.term = term;
        }

        public void addEntityInfo(EntityInfo ei) {
            entityInfos.add(ei);
        }

        public String getTerm() {
            return term;
        }

        public List<EntityInfo> getEntityInfos() {
            return entityInfos;
        }


        public Set<String> getCategories() {
            Set<String> categories = new HashSet<String>(7);
            for (EntityInfo ei : entityInfos) {
                if (ei.getCategory().length() > 0) {
                    categories.add(ei.getCategory());
                }
            }
            return categories;
        }

        public boolean hasCategory() {
            for (EntityInfo ei : entityInfos) {
                if (ei.getCategory().length() > 0) {
                    return true;
                }
            }
            return false;
        }

        public boolean hasAnyCategory(Set<String> allowedSet) {
            for (EntityInfo ei : entityInfos) {
                if (ei.getCategory().length() > 0) {
                    if (allowedSet.contains(ei.getCategory())) {
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean hasCategory(String category) {
            for (EntityInfo ei : entityInfos) {
                if (ei.getCategory().length() > 0) {
                    if (ei.getCategory().equals(category)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Keyword{");
            sb.append("term='").append(term).append('\'');
            sb.append(", entityInfos=").append(entityInfos);
            sb.append('}');
            return sb.toString();
        }

        public JSONObject toJSON() {
            JSONObject js = new JSONObject();
            js.put("term", term);
            JSONArray jsArr = new JSONArray();
            js.put("entityInfos", jsArr);
            for (EntityInfo ei : entityInfos) {
                jsArr.put(ei.toJSON());
            }
            return js;
        }

        public static Keyword fromJSON(JSONObject json) {
            String term = json.getString("term");
            Keyword kw = new Keyword(term);
            JSONArray jsArr = json.getJSONArray("entityInfos");

            for (int i = 0; i < jsArr.length(); i++) {
                JSONObject js = jsArr.getJSONObject(i);
                kw.addEntityInfo(EntityInfo.fromJSON(js));
            }
            return kw;
        }
    }

    public static class EntityInfo {
        final String contentLocation;
        final String id;
        final int start;
        final int end;
        final String category;

        public EntityInfo(String contentLocation, String id, int start, int end, String category) {
            this.contentLocation = contentLocation;
            this.id = id;
            this.start = start;
            this.end = end;
            this.category = category;
        }

        public String getContentLocation() {
            return contentLocation;
        }

        public String getId() {
            return id;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        public String getCategory() {
            return category;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("EntityInfo{");
            sb.append("contentLocation='").append(contentLocation).append('\'');
            sb.append(", id='").append(id).append('\'');
            sb.append(", start=").append(start);
            sb.append(", end=").append(end);
            sb.append(", category=").append(category);
            sb.append('}');
            return sb.toString();
        }

        public JSONObject toJSON() {
            JSONObject js = new JSONObject();
            js.put("contentLocation", contentLocation);
            js.put("id", id);
            js.put("start", start);
            js.put("end", end);
            js.put("category", category);
            return js;
        }

        public static EntityInfo fromJSON(JSONObject json) {
            String contentLocation = json.getString("contentLocation");
            String id = json.getString("id");
            int start = json.getInt("start");
            int end = json.getInt("end");
            String category = json.getString("category");
            return new EntityInfo(contentLocation, id, start, end, category);
        }
    }

}
