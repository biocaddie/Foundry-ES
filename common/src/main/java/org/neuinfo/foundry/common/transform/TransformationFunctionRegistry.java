package org.neuinfo.foundry.common.transform;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 4/17/15.
 */
public class TransformationFunctionRegistry {
    private static TransformationFunctionRegistry instance;
    Map<String, TransformationFunctionConfig> functionMap = new ConcurrentHashMap<String, TransformationFunctionConfig>();

    public synchronized static TransformationFunctionRegistry getInstance() {
        if (instance == null) {
            instance = new TransformationFunctionRegistry();
        }
        return instance;
    }

    private TransformationFunctionRegistry() {
        registerFunction("toStandardDate",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTransformation"));

        registerFunction("toStandardTime",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardTimeTransformation"));
        registerFunction("toStandardDateTime",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTimeTransformation"));
    }


    public void registerFunction(String name, TransformationFunctionConfig tfConfig) {
        functionMap.put(name, tfConfig);
    }


    public ITransformationFunction getFunction(String name) {
        TransformationFunctionConfig config = functionMap.get(name);
        if (config == null) {
            return null;
        }
        TransformationFunctionFactoryImpl factory = new TransformationFunctionFactoryImpl();
        return factory.create(config.getFqcn(), config.getConfigParamMap());
    }

    public static class TransformationFunctionConfig {
        String fqcn;
        Map<String, String> configParamMap = new HashMap<String, String>(7);

        public TransformationFunctionConfig(String fqcn) {
            this.fqcn = fqcn;
        }

        public void addConfigParam(String name, String value) {
            configParamMap.put(name, value);
        }

        public String getFqcn() {
            return fqcn;
        }

        public Map<String, String> getConfigParamMap() {
            return configParamMap;
        }
    }
}
