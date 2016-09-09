package org.neuinfo.foundry.common.transform;

import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/30/15.
 */
public class MappingEngine {
    List<Mapping> mappings;
    Map<String, Locator> locatorMap = new HashMap<String, Locator>(17);

    public MappingEngine(String mappingScript) {
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();
        TransformationLanguageInterpreter interpreter = new TransformationLanguageInterpreter(registry);
        interpreter.parse(mappingScript);
        this.mappings = interpreter.getMappings();
    }


    public void bootstrap(DocumentIngestionService dis) {
        if (locatorMap.isEmpty()) {
            for (Mapping mapping : mappings) {
                String key = mapping.getRefSourceID();
                if (!locatorMap.containsKey(key)) {
                    Locator locator = new Locator(mapping, dis);
                    locatorMap.put(key, locator);
                }
            }
        }
    }

    public void map(JSONObject docJSON) {
        for (Locator locator : locatorMap.values()) {
            locator.reset();
        }
        for (Mapping mapping : mappings) {
            Locator locator = locatorMap.get(mapping.getRefSourceID());
            List<String> sourceValues = locator.locate(docJSON, mapping);
            if (sourceValues == null) {
                System.err.println("Cannot locate " + mapping.getRefJsonPath());
            }
            if (!sourceValues.isEmpty()) {
                mapField(sourceValues, docJSON, mapping);
            }
        }

    }


    void mapField(List<String> sourceValues, JSONObject docJSON, Mapping mapping) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return;
        }
        if (sourceValues.size() == 1) {
            Map<String, JSONPathUtils.Parent> mappings2ParentMap = new HashMap<String, JSONPathUtils.Parent>();
            setJSONField(docJSON, mapping, sourceValues.get(0), mappings2ParentMap);
        } else {
            throw new RuntimeException("Only one-to-one mappings are supported currently!");
        }
    }

    private void setJSONField(JSONObject docJson, Mapping mapping, String value,
                              Map<String, JSONPathUtils.Parent> mappings2ParentMap) {
        JSONPathUtils.Parent parent = findParent(mapping, -1, mappings2ParentMap);
        JSONPathUtils.Parent newParent = JSONPathUtils.setJSONField2(docJson, mapping.getModelDestination(), value,
                parent);
        if (newParent != null) {
            addParent(mapping, newParent, mappings2ParentMap);
        }
    }


    void addParent(Mapping mapping, JSONPathUtils.Parent parent,
                   Map<String, JSONPathUtils.Parent> mapping2ParentMap) {
        String key;
        if (parent.getIdx() == -1) {
            key = mapping.getModelDestination();
        } else {
            key = mapping.getModelDestination() + parent.getIdx();
        }
        if (!mapping2ParentMap.containsKey(key)) {
            mapping2ParentMap.put(key, parent);
        }
    }

    JSONPathUtils.Parent findParent(Mapping mapping, int idx, Map<String, JSONPathUtils.Parent> mappings2ParentMap) {
        if (mappings2ParentMap.isEmpty()) {
            return null;
        }
        if (idx == -1) {
            return mappings2ParentMap.get(mapping.getModelDestination());
        } else {
            return mappings2ParentMap.get(mapping.getModelDestination() + idx);
        }
    }

}
