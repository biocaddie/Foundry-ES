package org.neuinfo.foundry.common.transform;

import java.util.List;

/**
 * Created by bozyurt on 4/16/15.
 */
public interface ITransformationFunction {

    public void addParam(String name, String value);

    public Result execute(String currentValue);

    public Result execute(List<String> currentValues);
}
