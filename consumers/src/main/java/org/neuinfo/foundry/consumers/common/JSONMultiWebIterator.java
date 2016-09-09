package org.neuinfo.foundry.consumers.common;

import org.apache.http.client.utils.URIBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.Utils;

import java.util.*;

/**
 * Created by bozyurt on 3/16/16.
 */
public class JSONMultiWebIterator implements Iterator<JSONMultiWebIterator.ParentJSONInfo> {
    List<String> ingestURLs;
    String docElJsonPath;
    List<ParentJSONInfo> pjiList;
    Iterator<ParentJSONInfo> iterator;

    public JSONMultiWebIterator(List<String> ingestURLs, List<String> ingestURLIds, String docElJsonPath, String idJsonPath)
            throws Exception {
        this.ingestURLs = ingestURLs;
        this.docElJsonPath = docElJsonPath;

        LinkedHashMap<String, ParentJSONInfo> childId2PJIMap = new LinkedHashMap<String, ParentJSONInfo>();
        JSONPathProcessor2 pathProcessor2 = new JSONPathProcessor2();
        for (int i = 0; i < ingestURLs.size(); i++) {
            String ingestURL = ingestURLs.get(i);
            String parentID = ingestURLIds.get(i);
            URIBuilder uriBuilder = new URIBuilder(ingestURL);
            String url = uriBuilder.build().toString();
            String content = Utils.sendGetRequest(url);
            JSONObject json = new JSONObject(content);
            List<JSONPathProcessor2.JPNode> docJsonNodes = pathProcessor2.find(docElJsonPath, json);
            if (!docJsonNodes.isEmpty() && docJsonNodes.size() == 1) {
                JSONArray jsArr = (JSONArray) docJsonNodes.get(0).getPayload();
                for (int j = 0; j < jsArr.length(); j++) {
                    JSONObject docJSON = jsArr.getJSONObject(j);
                    List<JSONPathProcessor2.JPNode> jpNodes = pathProcessor2.find(idJsonPath, docJSON);
                    if (!jpNodes.isEmpty()) {
                        String childId = jpNodes.get(0).getValue();
                        ParentJSONInfo pji = childId2PJIMap.get(childId);
                        if (pji == null) {
                            pji = new ParentJSONInfo(childId, docJSON);
                            childId2PJIMap.put(childId, pji);
                        }
                        pji.addParentID(parentID);
                    }
                }
            } else {
                for (JSONPathProcessor2.JPNode jpNode : docJsonNodes) {
                    if (jpNode.getPayload() instanceof JSONObject) {
                        JSONObject docJSON = (JSONObject) jpNode.getPayload();
                        List<JSONPathProcessor2.JPNode> jpNodes = pathProcessor2.find(idJsonPath, docJSON);
                        if (!jpNodes.isEmpty()) {
                            String childId = jpNodes.get(0).getValue();
                            ParentJSONInfo pji = childId2PJIMap.get(childId);
                            if (pji == null) {
                                pji = new ParentJSONInfo(childId, docJSON);
                                childId2PJIMap.put(childId, pji);
                            }
                            pji.addParentID(parentID);
                        }
                    } else {
                        JSONObject js = new JSONObject();
                        String childId = jpNode.getValue();
                        js.put("value", childId);
                        ParentJSONInfo pji = childId2PJIMap.get(childId);
                        if (pji == null) {
                            pji = new ParentJSONInfo(childId, js);
                            childId2PJIMap.put(childId, pji);
                        }
                        pji.addParentID(parentID);
                    }
                }
            }
        }
        this.pjiList = new ArrayList<ParentJSONInfo>(childId2PJIMap.size());
        for (ParentJSONInfo pji : childId2PJIMap.values()) {
            pjiList.add(pji);
        }
        childId2PJIMap.clear();
        this.iterator = this.pjiList.iterator();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public ParentJSONInfo next() {
        return this.iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static class ParentJSONInfo {
        JSONObject parent;
        String childID;
        Set<String> parentIDs;

        public ParentJSONInfo(String childID, JSONObject parent) {
            this.childID = childID;
            this.parent = parent;
        }

        void addParentID(String parentID) {
            if (parentIDs == null) {
                parentIDs = new HashSet<String>(11);
            }
            parentIDs.add(parentID);
        }

        public JSONObject getParent() {
            return parent;
        }

        public Set<String> getParentIDs() {
            return parentIDs;
        }

        public String getChildID() {
            return childID;
        }
    }
}
