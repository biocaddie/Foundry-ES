package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import org.neuinfo.foundry.common.transform.MappingEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 9/13/15.
 */
public class MappingRegistry {
    Map<String, MappingEngine> source2MappingEngineMap = new ConcurrentHashMap<String, MappingEngine>();
    private static MappingRegistry instance = new MappingRegistry();

    public static MappingRegistry getInstance() {
        return instance;
    }

    public void register(String sourceId, MappingEngine mappingEngine) {
        source2MappingEngineMap.put(sourceId, mappingEngine);
    }

    public MappingEngine getMappingEngine(String sourceId) {
        return this.source2MappingEngineMap.get(sourceId);
    }

    private MappingRegistry() {
    }
}
