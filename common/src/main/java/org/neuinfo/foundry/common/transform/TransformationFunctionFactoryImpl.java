package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Utils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by bozyurt on 4/17/15.
 */
public class TransformationFunctionFactoryImpl implements ITransformationFunctionFactory {
    @Override
    public ITransformationFunction create(String fqcn, Map<String,String> paramMap) {
        try {
            Class c = Class.forName(fqcn);
            ITransformationFunction function = (ITransformationFunction) c.newInstance();
            if (!paramMap.isEmpty()) {
                for(String configParamName : paramMap.keySet()) {
                    String setterName = Utils.prepSetterName(configParamName);
                    Method method = c.getMethod(setterName, String.class);
                    method.invoke(function, paramMap.get(configParamName));
                }
            }
            return function;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
