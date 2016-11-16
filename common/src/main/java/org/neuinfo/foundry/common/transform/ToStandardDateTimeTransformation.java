package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Assertion;

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
        if (dateFormat == null || dateFormat.endsWith("Z")) {
            try {
                Calendar calendar = DatatypeConverter.parseDate(currentValue);
                return new Result(sdf.format(calendar.getTime()));
            } catch (IllegalArgumentException x) {
                x.printStackTrace();
            }
        }
        if (dateFormat != null) {
            SimpleDateFormat origDateFormat = new SimpleDateFormat(dateFormat);
            try {
                Date date = origDateFormat.parse(currentValue);
                return new Result(sdf.format(date));
            } catch (ParseException x) {
                x.printStackTrace();
            }
        }
        return new Result("");
    }

    @Override
    public Result execute(List<String> currentValues) {
        return null;
    }
}
