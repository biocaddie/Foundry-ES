package org.neuinfo.foundry.common.transform;

import javax.xml.bind.DatatypeConverter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 1/20/16.
 */
public class ToStandardTimeTransformation implements ITransformationFunction {
    Map<String, String> params = new HashMap<String, String>(7);
    SimpleDateFormat sdf = new SimpleDateFormat("HHmmssZ");
    @Override
    public void addParam(String name, String value) {
        params.put(name, value);
    }

    @Override
    public Result execute(String currentValue) {
        String dateFormat = params.get("param1");

        if (dateFormat == null || dateFormat.endsWith("Z")) {
            try {
                // ISO8601
                Calendar calendar = DatatypeConverter.parseDateTime(currentValue);
                return new Result(sdf.format(calendar.getTime()));
            } catch (IllegalArgumentException x) {
                x.printStackTrace();
            }
        } else if (dateFormat != null) {
            SimpleDateFormat origDateFormat = new SimpleDateFormat(dateFormat);
            try {
                Date date = origDateFormat.parse(currentValue);
                return new Result(sdf.format(date));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public Result execute(List<String> currentValues) {
        return null;
    }
}
