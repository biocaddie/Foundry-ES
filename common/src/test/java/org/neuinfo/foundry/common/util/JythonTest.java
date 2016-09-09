package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 5/11/15.
 */
public class JythonTest extends TestCase {

    public JythonTest(String name) {
        super(name);
    }

    public void testArrayReturn() throws Exception {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.exec("import sys");
        interpreter.exec("");
        interpreter.set("value1", new PyString("DNA-RNA HYBRID"));
        interpreter.set("value2", new PyString("A-DNA/RNA,DOUBLE HELIX, DNA-RNA HYBRID"));
        interpreter.exec("arr =str.split(value1,',')\narr.extend(str.split(value2,','))\nresult=arr");
        PyObject result = interpreter.get("result");
        if (result instanceof PyList) {
            PyList list = (PyList) result;
            List<String> resultList = new ArrayList<String>(list.size());
            for (Iterator<Object> it = list.iterator(); it.hasNext(); ) {
                resultList.add(it.next().toString());
            }
            System.out.println(resultList);

        } else {
            String resultStr = result.asString();
            System.out.println("result:" + resultStr);
        }
    }
}
