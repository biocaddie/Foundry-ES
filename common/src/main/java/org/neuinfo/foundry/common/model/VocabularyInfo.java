package org.neuinfo.foundry.common.model;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by bozyurt on 3/10/16.
 */
public class VocabularyInfo {
    String identifier;
    List<String> synonyms;
    List<String> categories;
    List<String> acronyms;
    List<String> abbreviations;

    public VocabularyInfo(String identifier) {
        this.identifier = identifier;
    }

    public static VocabularyInfo fromJSON(String identifier, JSONObject sgJson) {
        VocabularyInfo vi = new VocabularyInfo(identifier);
        if (sgJson.has("synonyms")) {
            vi.synonyms = prepareList(sgJson, "synonyms");
        }
        if (sgJson.has("categories")) {
            vi.categories = prepareList(sgJson, "categories");
        }
        if (sgJson.has("acronyms")) {
            vi.acronyms = prepareList(sgJson, "acronyms");
        }
        if (sgJson.has("abbreviations")) {
            vi.abbreviations = prepareList(sgJson, "abbreviations");
        }
        return vi;
    }

    static List<String> prepareList(JSONObject sgJson, String arrName) {
        if (sgJson.has(arrName)) {
            JSONArray arr = sgJson.getJSONArray(arrName);
            if (arr.length() == 0) {
                return Collections.emptyList();
            } else {
                List<String> list = new ArrayList<String>(arr.length());
                for (int i = 0; i < arr.length(); i++) {
                    list.add(arr.getString(i));
                }
                return list;
            }
        }
        return null;
    }

    public String getIdentifier() {
        return identifier;
    }

    public List<String> getSynonyms() {
        return synonyms;
    }

    public List<String> getCategories() {
        return categories;
    }

    public List<String> getAcronyms() {
        return acronyms;
    }

    public List<String> getAbbreviations() {
        return abbreviations;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VocabularyInfo{");
        sb.append("identifier='").append(identifier).append('\'');
        sb.append("\n\t, synonyms=").append(synonyms);
        if (categories != null) {
            sb.append("\n\t, categories=").append(categories);
        }
        if (acronyms != null) {
            sb.append("\n\t, acronyms=").append(acronyms);
        }
        if (abbreviations != null) {
            sb.append("\n\t, abbreviations=").append(abbreviations);
        }
        sb.append("\n}");
        return sb.toString();
    }
}
