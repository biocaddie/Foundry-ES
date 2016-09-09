package org.neuinfo.foundry.common.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Keyword;
import org.neuinfo.foundry.common.util.CinergiXMLUtils.KeywordInfo;

import java.util.*;

/**
 * given a mongodb document generates an enhanced version of the original ISO XML document.
 * <p/>
 * Created by bozyurt on 2/11/15.
 */
public class ISOXMLGenerator {
    private Namespace gmd = Namespace.getNamespace("gmd", "http://www.isotc211.org/2005/gmd");

    public Element generate(DBObject docWrapper) throws Exception {
        DBObject originalDoc = (DBObject) docWrapper.get("OriginalDoc");

        DBObject data = (DBObject) docWrapper.get("Data");
        DBObject spatial = (DBObject) data.get("spatial");
        JSONObject originalDocJson = JSONUtils.toJSON((BasicDBObject) originalDoc, false);
        XML2JSONConverter converter = new XML2JSONConverter();
        Element docEl = converter.toXML(originalDocJson);
        if (spatial != null) {
            JSONObject spatialJson = JSONUtils.toJSON((BasicDBObject) spatial, false);
            docEl = addSpatialExtent(docEl, spatialJson);
        }


        Map<String, List<KeywordInfo>> category2KWIListMap = new HashMap<String, List<KeywordInfo>>(7);
        if (data.containsField("orgKeywords")) {
            DBObject kwDBO = (DBObject) data.get("orgKeywords");
            JSONArray jsArr = JSONUtils.toJSONArray((BasicDBList) kwDBO);
            for (int i = 0; i < jsArr.length(); i++) {
                JSONObject kwJson = jsArr.getJSONObject(i);
                Keyword kw = Keyword.fromJSON(kwJson);
                Set<String> categories = kw.getCategories();
                if (categories.size() == 1) {
                    String category = categories.iterator().next();
                    KeywordInfo kwi = new KeywordInfo(kw.getTerm(), category, null,
                            CinergiXMLUtils.KeywordType.Organization);
                    List<CinergiXMLUtils.KeywordInfo> kwiList = category2KWIListMap.get(category);
                    if (kwiList == null) {
                        kwiList = new ArrayList<KeywordInfo>(10);
                        category2KWIListMap.put(category, kwiList);
                    }
                    kwiList.add(kwi);
                }
            }
        }
        if (data.containsField("keywords")) {
            DBObject kwDBO = (DBObject) data.get("keywords");
            JSONArray jsArr = JSONUtils.toJSONArray((BasicDBList) kwDBO);
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

        }
        if (!category2KWIListMap.isEmpty()) {
            for (List<KeywordInfo> kwiList : category2KWIListMap.values()) {
                CinergiXMLUtils.filterPlurals(kwiList);
            }

            docEl = CinergiXMLUtils.addKeywords(docEl, category2KWIListMap);
        }
        // fix anchor problem if exists
        docEl = ISOXMLFixer.fixAnchorProblem(docEl);
        return docEl;
    }

    Element addSpatialExtent(Element docEl, JSONObject spatial) throws Exception {
        JSONArray boundingBoxes = spatial.getJSONArray("bounding_boxes");
        boolean hasBB = boundingBoxes.length() > 0;
        boolean hasBBFromPlaces = false;

        Element identificationInfo = docEl.getChild("identificationInfo", gmd);
        Element dataIdentification = identificationInfo.getChild("MD_DataIdentification", gmd);
        if (dataIdentification == null) {
            dataIdentification = new Element("MD_DataIdentification", gmd);
            identificationInfo.addContent(dataIdentification);
        }

        if (!hasBB) {
            JSONObject derivedBoundingBoxes = spatial.getJSONObject("derived_bounding_boxes_from_places");
            if (derivedBoundingBoxes.length() > 0) {
                for (String place : derivedBoundingBoxes.keySet()) {
                    JSONObject placeJson = derivedBoundingBoxes.getJSONObject(place);
                    JSONObject swJson = placeJson.getJSONObject("southwest");
                    JSONObject neJson = placeJson.getJSONObject("northeast");
                    String wbLongVal = String.valueOf(swJson.getDouble("lng"));
                    String sblatVal = String.valueOf(swJson.getDouble("lat"));
                    String ebLongVal = String.valueOf(neJson.getDouble("lng"));
                    String nbLatVal = String.valueOf(neJson.getDouble("lat"));
                    Element bbEl = CinergiXMLUtils.createBoundaryBox(wbLongVal, ebLongVal, sblatVal, nbLatVal, place);
                    dataIdentification.addContent(bbEl);
                }
                hasBBFromPlaces = true;
            }
        }
        if (!hasBB && !hasBBFromPlaces) {
            JSONObject derivedBoundingBoxes = spatial.getJSONObject("derived_bounding_boxes_from_derived_place");
            if (derivedBoundingBoxes.length() > 0) {
                for (String place : derivedBoundingBoxes.keySet()) {
                    JSONObject placeJson = derivedBoundingBoxes.getJSONObject(place);
                    JSONObject swJson = placeJson.getJSONObject("southwest");
                    JSONObject neJson = placeJson.getJSONObject("northeast");
                    String wbLongVal = String.valueOf(swJson.getDouble("lng"));
                    String sblatVal = String.valueOf(swJson.getDouble("lat"));
                    String ebLongVal = String.valueOf(neJson.getDouble("lng"));
                    String nbLatVal = String.valueOf(neJson.getDouble("lat"));
                    Element bbEl = CinergiXMLUtils.createBoundaryBox(wbLongVal, ebLongVal, sblatVal, nbLatVal, place);
                    dataIdentification.addContent(bbEl);
                }
            }
        }
        return docEl;
    }


}
