package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import javax.xml.bind.DatatypeConverter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 1/20/16.
 */
public class ToStandardDateTimeTransformation implements ITransformationFunction {
    Map<String, String> params = new HashMap<String, String>(7);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ");

    @Override
    public void addParam(String name, String value) {
        params.put(name, value);
    }

    @Override
    public Result execute(String currentValue) {
        String dateFormat = params.get("param1");
        Date date = Utils.parseDate(currentValue, dateFormat);
        if (date != null) {
             return new Result(sdf.format(date));
        }
        return new Result("");
    }

    @Override
    public Result execute(List<String> currentValues) {
        return null;
    }
}
