package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import org.neuinfo.foundry.common.transform.TransformationEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 5/5/15.
 */
public class TransformationRegistry {
    Map<String, TransformationEngine> source2TransformationsMap =
            new ConcurrentHashMap<String, TransformationEngine>();
    private static TransformationRegistry ourInstance = new TransformationRegistry();

    public static TransformationRegistry getInstance() {
        return ourInstance;
    }

    private TransformationRegistry() {
    }

    public void register(String sourceID, String transformScript) {
        TransformationEngine te = new TransformationEngine(transformScript);
        source2TransformationsMap.put(sourceID, te);
    }

    public TransformationEngine getTransformationEngine(String sourceID) {
        return source2TransformationsMap.get(sourceID);
    }
}
