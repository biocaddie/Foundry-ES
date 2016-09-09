package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Keyword;
import org.neuinfo.foundry.common.util.CinergiXMLUtils;
import org.neuinfo.foundry.common.util.CinergiXMLUtils.KeywordInfo;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.ProvenanceHelper.ProvData;

import java.util.*;

/**
 * Created by bozyurt on 2/4/15.
 */
public class EnhancerUtils {

    public static Map<String, List<KeywordInfo>> getKeywordsToBeAdded(JSONArray keywordsJson, JSONObject originalDocJson) {
        Map<String, List<KeywordInfo>> category2KWIListMap = new HashMap<String, List<KeywordInfo>>(7);
        for (int i = 0; i < keywordsJson.length(); i++) {
            JSONObject kwJson = keywordsJson.getJSONObject(i);
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
                CinergiXMLUtils.filterPlurals(kwiList);
            }
            XML2JSONConverter converter = new XML2JSONConverter();
            Element docEl = converter.toXML(originalDocJson);
            category2KWIListMap = CinergiXMLUtils.getNewKeywords(docEl, category2KWIListMap);
        }
        return category2KWIListMap;
    }

    public static JSONArray filter(JSONArray keywordsArr, Map<String, List<KeywordInfo>> category2KWIListMap,
                                   Map<String, Keyword> keywordMap) {
        JSONArray filtered = new JSONArray();
        for (int i = 0; i < keywordsArr.length(); i++) {
            JSONObject kwJson = keywordsArr.getJSONObject(i);
            String term = kwJson.getString("term");
            Keyword keyword = keywordMap.get(term);
            if (keyword != null) {
                if (hasMatch(category2KWIListMap, keyword)) {
                    filtered.put(kwJson);
                }
            }
        }
        return filtered;
    }

    public static JSONArray filterCategories(JSONArray keywordsArr, Set<String> excludeCategorySet) {
        JSONArray filtered = new JSONArray();
        for (int i = 0; i < keywordsArr.length(); i++) {
            JSONObject kwJson = keywordsArr.getJSONObject(i);
            Keyword k = Keyword.fromJSON(kwJson);
            if (!k.hasAnyCategory(excludeCategorySet)) {
                filtered.put(kwJson);
            }
        }
        return filtered;
    }

    public static boolean hasMatch(Map<String, List<KeywordInfo>> category2KWIListMap, Keyword keyword) {
        for (String category : category2KWIListMap.keySet()) {
            List<KeywordInfo> kwiList = category2KWIListMap.get(category);
            for (KeywordInfo kwi : kwiList) {
                if (kwi.getTerm().equals(keyword.getTerm())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void prepKeywordsProv(Map<String, List<KeywordInfo>> category2KWIListMap, ProvData provData) {
        if (category2KWIListMap == null || category2KWIListMap.isEmpty()) {
            provData.addModifiedFieldProv("No keywords are added");
            return;
        }
        for (String category : category2KWIListMap.keySet()) {
            List<KeywordInfo> kwiList = category2KWIListMap.get(category);
            StringBuilder sb = new StringBuilder(128);
            sb.append("Added keywords ");
            for (Iterator<KeywordInfo> iter = kwiList.iterator(); iter.hasNext(); ) {
                KeywordInfo kwi = iter.next();
                sb.append(kwi.getTerm());
                if (iter.hasNext()) {
                    sb.append(',');
                }
            }
            sb.append(" for category ").append(category);
            provData.addModifiedFieldProv(sb.toString().trim());
        }
    }
}
