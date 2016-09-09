package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Assertion;
import org.python.core.PyArray;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 4/14/15.
 */
public class Transformation {
    List<String> sourceColumnNames = new ArrayList<String>(1);
    String destColumnName;
    String script;
    String assignFromJsonPath;
    String constantValue;
    boolean unionOfSourceColumns = false;
    boolean constantTransform = false;
    ITransformationFunction transformationFunction;
    TransformationLanguageInterpreter.Node condition;

    public Transformation() {
    }

    public Transformation(TransformationLanguageInterpreter.Node condition) {
        this.condition = condition;
    }

    public Transformation(boolean constantTransform, TransformationLanguageInterpreter.Node condition) {
        this.constantTransform = constantTransform;
        this.condition = condition;
    }

    public ITransformationFunction getTransformationFunction() {
        return transformationFunction;
    }

    public void setTransformationFunction(ITransformationFunction transformationFunction) {
        this.transformationFunction = transformationFunction;
    }

    public List<String> getSourceColumnNames() {
        return sourceColumnNames;
    }


    public void addSourceColumnName(String sourceColumnName) {
        if (!sourceColumnNames.contains(sourceColumnName)) {
            sourceColumnNames.add(sourceColumnName);
        }
    }

    public TransformationLanguageInterpreter.Node getCondition() {
        return condition;
    }

    public String getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }

    public boolean isConstantTransform() {
        return constantTransform;
    }

    public boolean isUnionOfSourceColumns() {
        return unionOfSourceColumns;
    }

    public void setUnionOfSourceColumns(boolean unionOfSourceColumns) {
        this.unionOfSourceColumns = unionOfSourceColumns;
    }

    public String getAssignFromJsonPath() {
        return assignFromJsonPath;
    }

    public void setAssignFromJsonPath(String assignFromJsonPath) {
        this.assignFromJsonPath = assignFromJsonPath;
    }

    public String getDestColumnName() {
        return destColumnName;
    }

    public void setDestColumnName(String destColumnName) {
        this.destColumnName = destColumnName;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public Result executeNoColName(PythonInterpreter pythonInterpreter, String value) {
        return getResult(pythonInterpreter, value);
    }

    public Result execute(PythonInterpreter pythonInterpreter, String value) {
        Assertion.assertTrue(sourceColumnNames.size() == 1);
        pythonInterpreter.set("orig_colName", new PyString(sourceColumnNames.get(0)));
        return getResult(pythonInterpreter, value);
    }

    Result getResult(PythonInterpreter pythonInterpreter, String value) {
        pythonInterpreter.set("value", new PyString(value));
        pythonInterpreter.exec(script);
        PyObject result = pythonInterpreter.get("result");
        if (result instanceof PyList) {
            PyList list = (PyList) result;
            List<String> resultList = new ArrayList<String>(list.size());
            for (Iterator<Object> it = list.iterator(); it.hasNext(); ) {
                resultList.add(it.next().toString());
            }
            return new Result(resultList);
        } else {
            String resultStr = result.asString();
            return new Result(resultStr);
        }
    }

    public Result execute(PythonInterpreter pythonInterpreter, List<String> values) {
        Assertion.assertTrue(sourceColumnNames.size() > 1);
        Assertion.assertEquals(values.size(), sourceColumnNames.size());
        int idx = 1;
        Iterator<String> valueIter = values.iterator();
        for (String colName : sourceColumnNames) {
            pythonInterpreter.set("orig_colName" + idx, new PyString(colName));
            pythonInterpreter.set("value" + idx, new PyString(valueIter.next()));
            idx++;
        }
        pythonInterpreter.exec(script);
        PyObject result = pythonInterpreter.get("result");
        if (result instanceof PyList) {
            PyList list = (PyList) result;
            List<String> resultList = new ArrayList<String>(list.size());
            for (Iterator<Object> it = list.iterator(); it.hasNext(); ) {
                resultList.add(it.next().toString());
            }
            return new Result(resultList);
        } else {
            String resultStr = result.asString();
            return new Result(resultStr);
        }
    }
}
