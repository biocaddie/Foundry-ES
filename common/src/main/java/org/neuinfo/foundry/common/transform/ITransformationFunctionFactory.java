package org.neuinfo.foundry.common.transform;

import java.util.Map;

/**
 * Created by bozyurt on 4/17/15.
 */
public interface ITransformationFunctionFactory {
    public ITransformationFunction create(String fqcn, Map<String,String> paramMap);
}
