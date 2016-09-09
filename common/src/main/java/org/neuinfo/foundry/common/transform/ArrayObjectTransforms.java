package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Assertion;

import java.util.List;
import java.util.LinkedList;

/**
 * Created by bozyurt on 5/15/15.
 */
public class ArrayObjectTransforms {
    String key;
    int count = 0;
    List<Transformation> transformations = new LinkedList<Transformation>();

    public ArrayObjectTransforms(String key) {
        this.key = key;
    }

    public void addTransformation(Transformation transformation) {
        if (!transformations.contains(transformation)) {
            transformations.add(transformation);
        }
    }

    public int getNumOfTransformations() {
        return transformations.size();
    }

    public void incr() {
        count++;
    }

    public boolean isFirst() {
        return count < 2;
    }

    public void reset() {
        count = 0;
    }

    public static boolean isArrayObjectTransformationOrig(Transformation transformation) {
        String destColName = transformation.getDestColumnName();
        int idx = destColName.lastIndexOf('.');
        if (idx == -1 || idx < 2) {
            return false;
        }
        return destColName.charAt(idx - 2) == '[' && destColName.charAt(idx - 1) == ']';
    }

    public static boolean isArrayObjectTransformation(Transformation transformation) {
        String destColName = transformation.getDestColumnName();
        int idx = destColName.lastIndexOf('.');
        if (idx == -1 || idx < 2) {
            return false;
        }
        idx = destColName.indexOf("[]");
        if (idx == -1 || (idx+1) > destColName.length()) {
            return false;
        }
        int len = destColName.length();
        return ((idx+2) < len && destColName.charAt(idx+2) == '.');
    }

    public static String prepKey(Transformation transformation) {
        String destColName = transformation.getDestColumnName();
        int idx = destColName.lastIndexOf('.');
        Assertion.assertTrue(idx != -1);
        return destColName.substring(0, idx);
    }

}
