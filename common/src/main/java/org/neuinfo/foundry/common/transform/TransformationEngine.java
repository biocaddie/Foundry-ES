package org.neuinfo.foundry.common.transform;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.TransformationLanguageInterpreter.Node;
import org.neuinfo.foundry.common.transform.TransformationLanguageInterpreter.RelationalOp;
import org.neuinfo.foundry.common.transform.TransformationLanguageInterpreter.SimpleCondition;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor2.JPNode;
import org.neuinfo.foundry.common.util.Utils;
import org.python.util.PythonInterpreter;

import java.util.*;

/**
 * Created by bozyurt on 4/24/15.
 */
public class TransformationEngine {
    TransformationLanguageInterpreter interpreter;
    PythonInterpreter pythonInterpreter;
    Map<String, List<Transformation>> src2TransformationsMap = new HashMap<String, List<Transformation>>();
    Map<String, ArrayObjectTransforms> arrayObjectTransformsMap = new HashMap<String, ArrayObjectTransforms>(17);
    private final static Logger log = Logger.getLogger(TransformationEngine.class);

    public TransformationEngine(String transformScript) {
        // Profiler profiler = Profiler.getInstance("tr");
        //  profiler.entryPoint("registry");
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();
        // profiler.exitPoint("registry");
        interpreter = new TransformationLanguageInterpreter(registry);
        // profiler.entryPoint("parse");
        log.info(transformScript);
        interpreter.parse(transformScript);
        // profiler.exitPoint("parse");
        for (Transformation t : interpreter.getTransformations()) {
            if (t.isConstantTransform()) {
                continue;
            }
            String key = prepKey(t.getSourceColumnNames());
            List<Transformation> transformations = src2TransformationsMap.get(key);
            if (transformations == null) {
                transformations = new LinkedList<Transformation>();
                src2TransformationsMap.put(key, transformations);
            }
            transformations.add(t);
            if (ArrayObjectTransforms.isArrayObjectTransformation(t)) {
                String aoKey = ArrayObjectTransforms.prepKey(t);
                ArrayObjectTransforms aot = arrayObjectTransformsMap.get(aoKey);
                if (aot == null) {
                    aot = new ArrayObjectTransforms(aoKey);
                    arrayObjectTransformsMap.put(aoKey, aot);
                }
                aot.addTransformation(t);
            }
        }
        // profiler.entryPoint("python");
        this.pythonInterpreter = new PythonInterpreter();
        pythonInterpreter.exec("import sys");
        pythonInterpreter.exec("import re");
        // profiler.exitPoint("python");
    }


    public void transform(JSONObject sourceJson, JSONObject docJson) {
        Map<String, List<JPNode>> sourceValuesMap = new HashMap<String, List<JPNode>>();
        List<String> sourceJsonPaths = getSourceJsonpaths();
        for (String jsonPath : sourceJsonPaths) {
            JsonPathFieldExtractor extractor = new JsonPathFieldExtractor();
            extractor.extractValue2(sourceJson, jsonPath, sourceValuesMap);
        }
        for (ArrayObjectTransforms aot : arrayObjectTransformsMap.values()) {
            aot.reset();
        }
        // handle assignments
        for (Transformation t : interpreter.getTransformations()) {
            if (t.isConstantTransform()) {
                if (t.getCondition() != null) {
                    ConditionResult conditionResult = new ConditionResult();
                    handleCondition(sourceValuesMap, t.getCondition(), conditionResult);
                    if (!conditionResult.satisfied) {
                        continue;
                    }
                }
                assignField(docJson, t);
            }
        }
        for (List<Transformation> transformations : src2TransformationsMap.values()) {
            for (Transformation tr : transformations) {
                if (tr.getCondition() != null) {
                    ConditionResult condResult = new ConditionResult();
                    handleCondition(sourceValuesMap, tr.getCondition(), condResult);
                    if (!condResult.satisfied) {
                        // skip the transformation rule with unsatisfied condition
                        continue;
                    }
                }

                List<String> sourceColumnNames = tr.getSourceColumnNames();
                if (sourceColumnNames.size() == 1) {
                    String scn = normalizeSourceColumnName(sourceColumnNames.get(0));
                    List<JPNode> sourceValues = sourceValuesMap.get(scn);
                    if (Utils.isEmpty(tr.assignFromJsonPath)) {
                        transformField(sourceValues, docJson, tr);
                    } else {
                        JsonPathFieldExtractor extractor = new JsonPathFieldExtractor();
                        extractor.extractValue2(sourceJson, tr.assignFromJsonPath, sourceValuesMap);
                        List<JPNode> assignSourceValues = sourceValuesMap.get(tr.getAssignFromJsonPath());
                        if (assignSourceValues != null) {
                            transformDynamicField(sourceValues, docJson, tr, assignSourceValues);
                        }
                    }
                } else {
                    if (!tr.isUnionOfSourceColumns()) {
                        List<List<JPNode>> sourceValuesList = new ArrayList<List<JPNode>>(sourceColumnNames.size());
                        boolean complete = true;
                        for (String sourceColumnName : sourceColumnNames) {
                            String scn = normalizeSourceColumnName(sourceColumnName);
                            List<JPNode> sourceValues = sourceValuesMap.get(scn);
                            if (sourceValues == null || sourceValues.isEmpty()) {
                                complete = false;
                                break;
                            } else {
                                sourceValuesList.add(sourceValues);
                            }
                        }
                        if (complete) {
                            transformFieldMultiple(sourceValuesList, docJson, tr);
                        }
                    } else {
                        // union
                        List<JPNode> sourceValuesCombined = new ArrayList<JPNode>();
                        for (String sourceColumnName : sourceColumnNames) {
                            String scn = normalizeSourceColumnName(sourceColumnName);
                            List<JPNode> sourceValues = sourceValuesMap.get(scn);
                            if (sourceValues != null && !sourceValues.isEmpty()) {
                                JPNode jpNode = sourceValues.get(0);
                                if (jpNode.hasArrays() && jpNode.getIndices().length > 1) {
                                    new RuntimeException("Multi-dimensional arrays are not supported for UNION transforms!");
                                }
                                sourceValuesCombined.addAll(sourceValues);
                            }
                        }
                        if (!sourceValuesCombined.isEmpty()) {
                            transformFieldUnion(sourceValuesCombined, docJson, tr);
                        }
                    }
                }
            }
        }
        postprocess(docJson);
    }

    void postprocess(JSONObject docJson) {
        for(String key : new HashSet<String>(docJson.keySet())) {
            Object o = docJson.get(key);
            if (o instanceof JSONObject) {
                postprocess((JSONObject) o);
            } else if (o instanceof JSONArray) {
                JSONArray jsArr = (JSONArray) o;
                JSONArray newArr = cleanupJsonArray(jsArr);
                if (newArr != jsArr) {
                    docJson.put(key, newArr);
                }
                for(int i = 0; i < newArr.length(); i++) {
                    Object ao = newArr.get(i);
                    if (ao instanceof JSONObject) {
                        postprocess((JSONObject) ao);
                    }
                }
            }
        }

    }

    public static JSONArray cleanupJsonArray(JSONArray jsArr) {
        boolean found = false;
        for(int i = 0; i < jsArr.length(); i++) {
            if ( jsArr.get(i) == JSONObject.NULL) {
                found = true;
            }
        }
        if (!found) {
            return jsArr;
        }
        JSONArray arr = new JSONArray();
        for(int i = 0; i < jsArr.length(); i++) {
            if (jsArr.get(i) != JSONObject.NULL) {
               arr.put(jsArr.get(i));
            }
        }
        return arr;
    }

    public static String normalizeSourceColumnName(String sourceColumnName) {
        if (sourceColumnName.startsWith("xpath:")) {
            return sourceColumnName.substring(6);
        }
        return sourceColumnName;
    }


    void transformFieldUnion(List<JPNode> sourceValuesCombined, JSONObject docJson,
                             Transformation transformation) {

        int i = 0;
        for (JPNode jpNode : sourceValuesCombined) {
            if (transformation.getScript() != null) {
                Result result = transformation.executeNoColName(pythonInterpreter, jpNode.getValue());
                if (result != null) {
                    Assertion.assertTrue(!result.hasMultipleValues());
                    if (jpNode.hasArrays()) {
                        jpNode.getIndices()[0] = i;
                    }
                    setJSONField(docJson, transformation, result, jpNode);
                }
            } else {
                if (jpNode.hasArrays()) {
                    jpNode.getIndices()[0] = i;
                }
                setJSONField(docJson, transformation, jpNode);
            }
            i++;
        }
    }

    void transformFieldMultiple(List<List<JPNode>> sourceValuesList, JSONObject docJson,
                                Transformation transformation) {
        if (sourceValuesList == null || sourceValuesList.isEmpty()) {
            return;
        }
        int numOfValueSets = sourceValuesList.get(0).size();
        List<List<JPNode>> sourceColumnsValueList = new ArrayList<List<JPNode>>(numOfValueSets);
        int numCols = sourceValuesList.size();
        for (int i = 0; i < numOfValueSets; i++) {
            sourceColumnsValueList.add(new ArrayList<JPNode>(numCols));
        }
        for (int i = 0; i < numOfValueSets; i++) {
            for (List<JPNode> sourceValues : sourceValuesList) {
                List<JPNode> sourceColumnsValue = sourceColumnsValueList.get(i);
                sourceColumnsValue.add(sourceValues.get(i));
            }
        }
        if (sourceColumnsValueList.size() == 1) {
            if (transformation.getScript() != null) {
                List<String> values = new ArrayList<String>(sourceValuesList.get(0).size());
                for (JPNode jpNode : sourceColumnsValueList.get(0)) {
                    values.add(jpNode.getValue());
                }

                Result result = transformation.execute(pythonInterpreter, values);
                if (result != null) {
                    // assumption: use the first source json path match for any array index information
                    JPNode jpNode = sourceColumnsValueList.get(0).get(0);
                    setJSONField(docJson, transformation, result, jpNode);
                }
            } else {
                throw new RuntimeException("Unsupported transformation!");
            }
        } else {
            for (List<JPNode> sourceColumnsValue : sourceColumnsValueList) {
                if (transformation.getScript() != null) {
                    List<String> values = new ArrayList<String>(sourceColumnsValue.size());
                    for (JPNode jpNode : sourceColumnsValue) {
                        values.add(jpNode.getValue());
                    }
                    Result result = transformation.execute(pythonInterpreter, values);
                    if (result != null) {
                        // assumption: use the first source json path match for any array index information
                        JPNode jpNode = sourceColumnsValue.get(0);
                        setJSONField(docJson, transformation, result, jpNode);
                    }
                } else {
                    throw new RuntimeException("Unsupported transformation!");
                }
            }
        }
    }


    void transformDynamicField(List<JPNode> sourceValues, JSONObject docJson, Transformation transformation,
                               List<JPNode> assignFromJsonPathValues) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return;
        }
        if (sourceValues.size() > 1) {
            Assertion.assertTrue(transformation.getScript() == null);
            Map<JPNode, List<JPNode>> varName2SourceValuesMap = new LinkedHashMap<JPNode, List<JPNode>>();
            for (JPNode varNameNode : assignFromJsonPathValues) {
                Assertion.assertTrue(varNameNode.hasArrays() && varNameNode.getIndices().length == 1);
                int varIdx = varNameNode.getIndices()[0];
                List<JPNode> values = new ArrayList<JPNode>(1);
                varName2SourceValuesMap.put(varNameNode, values);
                for (Iterator<JPNode> iter = sourceValues.iterator(); iter.hasNext(); ) {
                    JPNode sourceValue = iter.next();
                    Assertion.assertTrue(sourceValue.hasArrays() && sourceValue.getIndices().length >= 1);
                    int firstIdx = sourceValue.getIndices()[0];
                    if (varIdx == firstIdx) {
                        values.add(sourceValue);
                        iter.remove();
                    }
                }
            }
            for (JPNode varNameNode : varName2SourceValuesMap.keySet()) {
                List<JPNode> values = varName2SourceValuesMap.get(varNameNode);
                for (JPNode value : values) {
                    JPNode singleValue = new JPNode(value.getPayload());
                    setDynamicJsonField(docJson, transformation, singleValue, varNameNode.getValue());
                }
            }
        } else {
            if (transformation.getScript() != null) {
                Result result = transformation.execute(pythonInterpreter, sourceValues.get(0).getValue());
                if (result != null) {
                    setDynamicJSONField(docJson, transformation, result, sourceValues.get(0), assignFromJsonPathValues.get(0).getValue());
                }
            } else {
                if (transformation.getTransformationFunction() != null) {
                    Result result = transformation.getTransformationFunction().execute(sourceValues.get(0).getValue());
                    setDynamicJSONField(docJson, transformation, result, sourceValues.get(0),
                            assignFromJsonPathValues.get(0).getValue());
                } else {
                    setDynamicJsonField(docJson, transformation, sourceValues.get(0),
                            assignFromJsonPathValues.get(0).getValue());
                }
            }
        }
    }

    void assignField(JSONObject docJson, Transformation transformation) {
        JPNode sourceValue = new JPNode(transformation.getConstantValue());
        setJSONField(docJson, transformation, sourceValue);
    }


    boolean isConditionSatisified(Map<String, List<JPNode>> sourceValuesMap, Node condition) {
        ConditionResult cr = new ConditionResult();
        handleCondition(sourceValuesMap, condition, cr);

        return cr.satisfied;
    }

    void handleCondition(Map<String, List<JPNode>> sourceValuesMap, Node condition, ConditionResult result) {
        Node leftOperand = condition.getLeftOperand();
        Node rightOperand = condition.getRightOperand();
        TransformationLanguageInterpreter.LogicalOp logicalOp = condition.getLogicalOp();
        boolean leftResult = true;
        boolean rightResult = true;
        if (leftOperand instanceof SimpleCondition) {
            ConditionResult tcr = new ConditionResult();
            handleSimpleCondition(sourceValuesMap, tcr, (SimpleCondition) leftOperand);
            leftResult = tcr.satisfied;
        } else {
            ConditionResult tcr = new ConditionResult();
            handleCondition(sourceValuesMap, leftOperand, tcr);
            leftResult = tcr.satisfied;
        }
        if (rightOperand != null) {
            if (rightOperand instanceof SimpleCondition) {
                ConditionResult tcr = new ConditionResult();
                handleSimpleCondition(sourceValuesMap, tcr, (SimpleCondition) rightOperand);
                rightResult = tcr.satisfied;
            } else {
                ConditionResult tcr = new ConditionResult();
                handleCondition(sourceValuesMap, leftOperand, tcr);
                rightResult = tcr.satisfied;

            }
        }
        if (rightOperand == null) {
            result.satisfied = leftResult;
        } else {
            if (logicalOp == TransformationLanguageInterpreter.LogicalOp.AND) {
                result.satisfied = leftResult && rightResult;
            } else {
                result.satisfied = leftResult || rightResult;
            }
        }
    }

    private void handleSimpleCondition(Map<String, List<JPNode>> sourceValuesMap, ConditionResult result, SimpleCondition leftOperand) {
        SimpleCondition sc = leftOperand;
        String lhs = sc.getLhs();
        List<JPNode> jpNodes = sourceValuesMap.get(lhs);
        RelationalOp relOp = sc.getRelOp();
        if (relOp == RelationalOp.EXISTS) {
            result.satisfied = jpNodes != null && !jpNodes.isEmpty();
        } else if (relOp == RelationalOp.NOT_EXISTS) {
            result.satisfied = jpNodes == null || jpNodes.isEmpty();
        } else {
            if (jpNodes == null || jpNodes.isEmpty()) {
                result.satisfied = false;
            } else {
                // Assumption: Single value
                JPNode jpNode = jpNodes.get(0);
                String value = jpNode.getValue();
                if (relOp == RelationalOp.EQ) {
                    result.satisfied = value.equals(sc.getRhs());
                } else if (relOp == RelationalOp.NE) {
                    result.satisfied = !value.equals(sc.getRhs());
                } else if (relOp == RelationalOp.LIKE) {
                    result.satisfied = Utils.handleLike(value, sc.getRhs());
                } else if (relOp == RelationalOp.NOT_LIKE) {
                    result.satisfied = !Utils.handleLike(value, sc.getRhs());
                } else {
                    // numeric
                    if (!Utils.isNumber(value) || !Utils.isNumber(sc.getRhs())) {
                        result.satisfied = false;
                    } else {
                        Double dblValue = Utils.toDouble(value);
                        Double refValue = Utils.toDouble(sc.getRhs());
                        if (relOp == RelationalOp.GT) {
                            result.satisfied = dblValue > refValue;
                        } else if (relOp == RelationalOp.LT) {
                            result.satisfied = dblValue < refValue;
                        } else if (relOp == RelationalOp.GTE) {
                            result.satisfied = dblValue >= refValue;
                        } else if (relOp == RelationalOp.LTE) {
                            result.satisfied = dblValue <= refValue;
                        }
                    }
                }
            }

        }
    }


    static class ConditionResult {
        boolean satisfied = true;
    }

    void transformField(List<JPNode> sourceValues, JSONObject docJson, Transformation transformation) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return;
        }
        if (sourceValues.size() == 1) {
            if (transformation.getScript() != null) {
                JPNode jpNode = sourceValues.get(0);
                Result result = transformation.execute(pythonInterpreter, jpNode.getValue());
                if (result != null) {
                    setJSONField(docJson, transformation, result, jpNode);

                }
            } else {
                JPNode sourceValue = sourceValues.get(0);
                if (transformation.getTransformationFunction() != null) {
                    Result result = transformation.getTransformationFunction().execute(
                            sourceValue.getValue());
                    Assertion.assertTrue(!result.hasMultipleValues());
                    sourceValue.setPayload(result.getValue());
                    setJSONField(docJson, transformation, sourceValue);
                } else {
                    setJSONField(docJson, transformation, sourceValue);
                }
            }
        } else {
            if (transformation.isJoinTransform()) {
                if (transformation.getScript() != null) {
                    List<String> joinList = new ArrayList<String>(sourceValues.size());
                    for (JPNode sourceValue : sourceValues) {
                        joinList.add(sourceValue.getValue());
                    }
                    Result result = transformation.executeJoin(pythonInterpreter, joinList);
                    if (result != null) {
                        JPNode jsv = new JPNode(sourceValues.get(0));
                        jsv.setPayload( result.getValue());
                        setJSONField(docJson, transformation, jsv);
                    }
                } else {
                    StringBuilder sb = new StringBuilder(128);
                    for (JPNode sourceValue : sourceValues) {
                        sb.append(sourceValue.getValue()).append(' ');
                    }
                    String joinedValue = sb.toString().trim();
                    JPNode jsv = new JPNode(sourceValues.get(0));
                    jsv.setPayload(joinedValue);
                    setJSONField(docJson, transformation, jsv);
                }
            } else {
                for (JPNode sourceValue : sourceValues) {
                    if (transformation.getScript() != null) {
                        Result result = transformation.execute(pythonInterpreter, sourceValue.getValue());
                        if (result != null) {
                            Assertion.assertTrue(!result.hasMultipleValues());
                            sourceValue.setPayload(result.getValue());
                            setJSONField(docJson, transformation, sourceValue);
                        }
                    } else {
                        if (transformation.getTransformationFunction() != null) {
                            Result result = transformation.getTransformationFunction().execute(sourceValue.getValue());
                            Assertion.assertTrue(!result.hasMultipleValues());
                            sourceValue.setPayload(result.getValue());
                            setJSONField(docJson, transformation, sourceValue);
                        } else {
                            setJSONField(docJson, transformation, sourceValue);
                        }
                    }
                }
            }
        }
    }

    private void setDynamicJsonField(JSONObject docJson, Transformation transformation,
                                     JPNode value, String assignJsonPathValue) {
        String destJsonPath = transformation.getDestColumnName();
        boolean isArray = destJsonPath.endsWith("[]");
        destJsonPath = destJsonPath.replaceFirst("\\.[^\\.]+$", "." + assignJsonPathValue);
        if (isArray) {
            destJsonPath += "[]";
        }
        JSONPathUtils.setJSONField2(docJson, destJsonPath, value);
    }

    private void setJSONField(JSONObject docJson, Transformation transformation,
                              JPNode value) {
        JSONPathUtils.setJSONField2(docJson, transformation.getDestColumnName(), value);
    }

    private void setDynamicJSONField(JSONObject docJson, Transformation transformation,
                                     Result result, JPNode valueNode, String assignFromJsonPathValue) {
        if (!result.hasMultipleValues()) {
            String destJsonPath = transformation.getDestColumnName();
            destJsonPath = destJsonPath.replaceFirst("\\.[^\\.]+$", "." + assignFromJsonPathValue);
            valueNode.setPayload(result.getValue());
            JSONPathUtils.setJSONField2(docJson, destJsonPath, valueNode);
        } else {
            throw new RuntimeException("Only single value dynamic field transformations is supported!");
        }
    }


    private void setJSONField(JSONObject docJson, Transformation transformation,
                              Result result, JPNode valueNode) {
        if (!result.hasMultipleValues()) {
            JPNode tvn = new JPNode(valueNode);
            tvn.setPayload(result.getValue());
            // valueNode.setPayload(result.getValue());
            JSONPathUtils.setJSONField2(docJson, transformation.getDestColumnName(), tvn);
        } else {
            for (String value : result.getValues()) {
                JPNode tvn = new JPNode(valueNode);
                tvn.setPayload(value);
                JSONPathUtils.setJSONField2(docJson, transformation.getDestColumnName(), tvn);
            }
        }
    }

    public List<String> getSourceJsonpaths() {
        List<String> sourceJsonPathList = new ArrayList<String>();
        Set<String> uniqSet = new HashSet<String>();
        for (List<Transformation> transformations : src2TransformationsMap.values()) {
            for (Transformation t : transformations) {
                if (t.getCondition() != null) {
                    collectConditionalPaths(t.getCondition(), sourceJsonPathList, uniqSet);
                }
                if (!t.isConstantTransform()) {
                    for (String sourceColumnName : t.getSourceColumnNames()) {
                        if (!uniqSet.contains(sourceColumnName)) {
                            sourceJsonPathList.add(sourceColumnName);
                            uniqSet.add(sourceColumnName);
                        }
                    }
                }
            }
        }
        // constant statements needs special treatment
         for (Transformation t : interpreter.getTransformations()) {
             if (t.isConstantTransform() && t.getCondition() != null) {
                 collectConditionalPaths(t.getCondition(), sourceJsonPathList, uniqSet);
             }
         }
        return sourceJsonPathList;
    }

    void collectConditionalPaths(Node condition, List<String> sourceJsonPathList, Set<String> uniqSet) {
        Node leftOperand = condition.getLeftOperand();
        Node rightOperand = condition.getRightOperand();

        if (leftOperand instanceof SimpleCondition) {
            SimpleCondition sc = (SimpleCondition) leftOperand;
            String s = sc.getLhs();
            if (!uniqSet.contains(s)) {
                sourceJsonPathList.add(s);
                uniqSet.add(s);
            }
        } else {
            collectConditionalPaths(leftOperand, sourceJsonPathList, uniqSet);
        }
        if (rightOperand != null) {
            if (rightOperand instanceof SimpleCondition) {
                SimpleCondition sc = (SimpleCondition) rightOperand;
                String s = sc.getLhs();
                if (!uniqSet.contains(s)) {
                    sourceJsonPathList.add(s);
                    uniqSet.add(s);
                }
            } else {
                collectConditionalPaths(condition.getRightOperand(), sourceJsonPathList, uniqSet);
            }
        }
    }

    public static String prepKey(List<String> sourceColumnNames) {
        StringBuilder sb = new StringBuilder(128);
        for (Iterator<String> it = sourceColumnNames.iterator(); it.hasNext(); ) {
            String sourceColumnName = it.next();
            sb.append(sourceColumnName);
            if (it.hasNext()) {
                sb.append('|');
            }
        }
        return sb.toString();
    }

}
