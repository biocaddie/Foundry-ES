package org.neuinfo.foundry.common.transform;

import org.python.core.PyDictionary;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/14/15.
 */
public class PythonTransformationFunction implements ITransformationFunction {
    String name;
    String functionBody;
    PythonInterpreter pythonInterpreter;
    Map<String, String> params = new HashMap<String, String>(11);

    public PythonTransformationFunction(String name, String functionBody, PythonInterpreter pythonInterpreter) {
        this.name = name;
        this.functionBody = functionBody;
        this.pythonInterpreter = pythonInterpreter;
    }


    @Override
    public void addParam(String name, String value) {
        params.put(name, value);
    }


    @Override
    public Result execute(String currentValue) {
        PyDictionary dict = new PyDictionary();
        for (String name : params.keySet()) {
            String value = params.get(name);
            dict.put(new PyString(name), new PyString(value));
        }
        dict.put("value", currentValue);
        pythonInterpreter.set("paramMap", dict);

        pythonInterpreter.exec(functionBody);
        pythonInterpreter.exec("result = " + name + "(paramMap)");
        PyObject result = pythonInterpreter.get("result");
        String resultStr = result.asString();
        return new Result(resultStr);
    }

    @Override
    public Result execute(List<String> currentValues) {
        PyDictionary dict = new PyDictionary();
        for (String name : params.keySet()) {
            String value = params.get(name);
            dict.put(new PyString(name), new PyString(value));

        }
        int idx = 1;
        for (String curValue : currentValues) {
            dict.put("value" + idx, curValue);
            idx++;
        }

        pythonInterpreter.set("paramMap", dict);

        pythonInterpreter.exec(functionBody);
        pythonInterpreter.exec("result = " + name + "(paramMap)");
        PyObject result = pythonInterpreter.get("result");
        String resultStr = result.asString();
        return new Result(resultStr);
    }
}
