package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import javax.xml.bind.DatatypeConverter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 4/16/15.
 */
public class ToStandardDateTransformation implements ITransformationFunction {
    Map<String, String> params = new HashMap<String, String>(11);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void addParam(String name, String value) {
        params.put(name, value);
    }

    @Override
    public Result execute(String currentValue) {
        String dateFormat = params.get("param1");
        Assertion.assertNotNull(dateFormat);
        Date date = Utils.parseDate(currentValue, dateFormat);
        if (date != null) {
            return new Result(sdf.format(date));
        }
        /*
        if (dateFormat.endsWith("Z")) {
            try {
                // ISO8601
                Calendar calendar = DatatypeConverter.parseDateTime(currentValue);
                return new Result(sdf.format(calendar.getTime()));
            } catch (IllegalArgumentException x) {
                x.printStackTrace();
            }
        }
        SimpleDateFormat origDateFormat = new SimpleDateFormat(dateFormat);
        try {
            Date date = origDateFormat.parse(currentValue);
            return new Result(sdf.format(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        */
        return new Result("");
    }

    @Override
    public Result execute(List<String> currentValues) {
        return null;
    }
}
