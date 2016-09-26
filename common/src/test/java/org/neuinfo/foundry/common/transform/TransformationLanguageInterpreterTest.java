package org.neuinfo.foundry.common.transform;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bozyurt on 9/19/16.
 */
public class TransformationLanguageInterpreterTest {

    @Before
    public void setup() {
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();
        registry.registerFunction("toStandardDate",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTransformation"));
    }


    @Test
    public void testMultipleRuleParsing() {
        String ts = "/* some comment */ " + "transform column \"Stk #\" to \"id\" apply {{result=orig_colName + '_1'}};"
                + "transform column \"Date added\" to \"date_added\" apply toStandardDate(\"MM/dd/yyyy\");";
        TransformationLanguageInterpreter interpreter = getTransformationLanguageInterpreter(ts);
        assertEquals(interpreter.getTransformations().size(), 2);
    }

    @Test
    public void testLetStatementParsing() {
        String ts = "let \"datasetDistributions[0].accessType\" = \"download\";";
        TransformationLanguageInterpreter interpreter = getTransformationLanguageInterpreter(ts);
        assertEquals(interpreter.getTransformations().size(), 1);
    }

    @Test
    public void testIfStatementParsing() {
        String ts = "if ( \"$.'PDBx:datablock'\" exists or \"$.'Cell ID'\" = \"45\" ) and \"$.x\" <> \"a\" then transform column \"Date added\" to \"date_added\" apply toStandardDate(\"MM/dd/yyyy\");\"";
        TransformationLanguageInterpreter interpreter = getTransformationLanguageInterpreter(ts);
        assertEquals(interpreter.getTransformations().size(), 1);
    }

    @Test
    public void testJoinStatementParsing() {
        String ts = "join \"$.'record'.'metadata'.'codeBook'.'stdyDscr'.'dataAccs'.'useStmt'.'conditions'.'p'[*].'_$'\" " +
                "to \"datasetDistribution[0].license\" apply {{  result= ' '.join(value) }};";
        TransformationLanguageInterpreter interpreter = getTransformationLanguageInterpreter(ts);
        assertEquals(interpreter.getTransformations().size(), 1);
    }

    private TransformationLanguageInterpreter getTransformationLanguageInterpreter(String ts) {
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();
        TransformationLanguageInterpreter interpreter = new TransformationLanguageInterpreter(registry);
        interpreter.parse(ts);
        return interpreter;
    }
}
