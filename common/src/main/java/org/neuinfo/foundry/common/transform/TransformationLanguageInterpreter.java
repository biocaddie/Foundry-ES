package org.neuinfo.foundry.common.transform;

import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.util.Assertion;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

import static org.neuinfo.foundry.common.transform.TokenType.*;

import java.util.*;

/**
 * <pre>
 *     [IF {conditional} THEN] TRANSFORM COLUMN {origColName} TO {newColName} [ASSIGN NAME FROM {columnJsonPath}] [APPLY {{transformScript}}];
 *     [IF {conditional} THEN] TRANSFORM COLUMNS {origColName1,...origColNameN} TO {newColName} [APPLY {{transformScript}}];
 *     [IF {conditional} THEN] TRANSFORM UNION {origColName1,...origColNameN} TO {newColName} [APPLY {{transformScript}}];
 *     JOIN {origColName1,...origColNameN} TO {newColName} [APPLY {{transformScript}}];
 *     [IF {conditional} THEN] LET {newColName} = {Constant};
 *     MAP {refSourceID}[{jsonPath4PK}].{jsonPath} TO {model JSON Path};
 *
 * </pre>
 * Created by bozyurt on 4/13/15.
 */
public class TransformationLanguageInterpreter {
    List<Transformation> transformations = new ArrayList<Transformation>(10);
    List<Mapping> mappings = new ArrayList<Mapping>(10);
    TransformationFunctionRegistry registry;
    private final static Logger log = Logger.getLogger(TransformationEngine.class);

    public TransformationLanguageInterpreter(TransformationFunctionRegistry registry) {
        this.registry = registry;
    }

    public void parse(String transformScript) {
        Tokenizer tokenizer = new Tokenizer(transformScript);
        TokenInfo ti;
        while ((ti = tokenizer.nextToken()) != null) {
            log.debug(ti);
            switch (ti.getType()) {

                case TRANSFORM:
                    Transformation transformation = parseTransformStatement(tokenizer, null);
                    transformations.add(transformation);
                    break;
                case MAP:
                    Mapping mapping = parseMappingStatement(tokenizer);
                    mappings.add(mapping);
                    break;
                case LET:
                    Transformation transformation1 = parseAssignmentStatement(tokenizer, null);
                    transformations.add(transformation1);
                    break;
                case IF:
                    Transformation tr = parseConditionalStatement(tokenizer);
                    transformations.add(tr);
                    break;
                case JOIN:
                    Transformation jr = parseJoinStatement(tokenizer, null);
                    transformations.add(jr);
            }
        }
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public List<Mapping> getMappings() {
        return mappings;
    }

    public static void errorIfNot(boolean expressionVal, String msg, int lineNo) {
        if (!expressionVal) {
            throw new SyntaxException(String.format("%s at line %d!", msg, lineNo));
        }
    }

    Mapping parseMappingStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && (ti.getType() == STRING || ti.getType() == LITERAL),
                "Expected a string or a string literal after map", tokenizer.getLineNo());
        Mapping mapping = new Mapping();
        // mapping.setFromColumnName(ti.getTokValue());
        mapping.setRefSourceID(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == LEFT_RB, "'[' is expected", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after '['", tokenizer.getLineNo());
        mapping.setJsonPathForPK(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == RIGHT_RB, "']' is expected", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == PERIOD, "'.' is expected after ']'", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after '.'", tokenizer.getLineNo());
        mapping.setRefJsonPath(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == TO, "'to' is expected", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after the keyword 'to'", tokenizer.getLineNo());
        mapping.setModelDestination(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
        return mapping;
    }

    Transformation parseAssignmentStatement(Tokenizer tokenizer, Node condition) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "A string literal is expected after the 'let' keyword", tokenizer.getLineNo());
        Transformation transformation = new Transformation(true, condition);
        transformation.setDestColumnName(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == EQUALS,
                "'=' is expected after string literal", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "A string literal is expected after '='", tokenizer.getLineNo());
        transformation.setConstantValue(ti.getTokValue());
        return transformation;

    }

    Transformation parseConditionalStatement(Tokenizer tokenizer) {
        Node condition = parseCondition(tokenizer);
        TokenInfo ti = tokenizer.nextToken();
        if (ti.getType() == TRANSFORM) {
            return parseTransformStatement(tokenizer, condition);
        } else if (ti.getType() == LET) {
            return parseAssignmentStatement(tokenizer, condition);
        }
        return null;
    }

    Node parseCondition(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        if (ti.getType() == LEFT_PAREN) {
            Node condition = parseCondition(tokenizer);
            TokenInfo nextTI = tokenizer.nextToken();
            errorIfNot(nextTI != null && nextTI.getType() == RIGHT_PAREN,
                    "A closing parenthesis is expected in the if statement", tokenizer.getLineNo());
            nextTI = tokenizer.nextToken();
            if (nextTI != null && nextTI.getType() != THEN) {
                if (nextTI.getType() == AND || nextTI.getType() == OR) {
                    Node leftOperand = condition;
                    Node rightOperand = parseCondition(tokenizer);
                    BinaryCondition bc = new BinaryCondition();
                    bc.setLeftOperand(leftOperand);
                    bc.setRightOperand(rightOperand);
                    bc.setLogicalOp(nextTI.getType() == AND ? LogicalOp.AND : LogicalOp.OR);
                    return bc;
                } else {
                    throw new RuntimeException("Should not happen!");
                }
            } else {
                return condition;
            }

        } else {
            tokenizer.pushBack(ti);
            Node leftOperand = parseSimpleCondition(tokenizer);
            TokenInfo ti2 = tokenizer.nextToken();
            errorIfNot(ti2 != null && (ti2.getType() == AND || ti2.getType() == OR || ti2.getType() == THEN
                            || ti2.getType() == RIGHT_PAREN),
                    "One of 'and', 'or', 'then' or ')' is expected after a condition", tokenizer.getLineNo());
            if (ti2.getType() == THEN) {
                return leftOperand;
            } else if (ti2.getType() == RIGHT_PAREN) {
                tokenizer.pushBack(ti2);
                return leftOperand;
            } else {
                Node rightOperand = parseCondition(tokenizer);
                BinaryCondition condition = new BinaryCondition();
                condition.setLeftOperand(leftOperand);
                condition.setRightOperand(rightOperand);
                condition.setLogicalOp(ti2.getType() == AND ? LogicalOp.AND : LogicalOp.OR);
                return condition;
            }
        }
    }


    Node parseSimpleCondition(Tokenizer tokenizer) {
        SimpleCondition condition = null;
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && (ti.getType() == STRING),
                "Needs a string as the left hand side of a condition", tokenizer.getLineNo());
        TokenInfo ti2 = tokenizer.nextToken();
        TokenType type = ti2.getType();
        errorIfNot(ti2 != null && (type == EQUALS || type == LT || type == GT || type == GTE || type == LTE || type == NE
                        || type == EXISTS || type == LIKE || type == NOT),
                "Needs a relational operator after the left hand side of a condition", tokenizer.getLineNo());
        if (type == NOT) {
            TokenInfo nextTi = tokenizer.nextToken();
            errorIfNot(nextTi != null && (nextTi.getType() == EXISTS || nextTi.getType() == LIKE),
                    "Needs either 'exists' or 'like' after 'not' in a condition", tokenizer.getLineNo());
            if (type == EXISTS) {
                condition = new SimpleCondition(ti.getTokValue(), null, RelationalOp.NOT_EXISTS);
            } else {
                TokenInfo ti3 = tokenizer.nextToken();
                errorIfNot(ti3 != null && (ti3.getType() == STRING),
                        "Needs a string as the right hand side of a condition", tokenizer.getLineNo());
                condition = new SimpleCondition(ti.getTokValue(), ti3.getTokValue(), RelationalOp.NOT_LIKE);
            }

        } else if (type != EXISTS) {
            TokenInfo ti3 = tokenizer.nextToken();
            errorIfNot(ti3 != null && (ti3.getType() == STRING),
                    "Needs a string as the right hand side of a condition", tokenizer.getLineNo());
            RelationalOp relOp = toRelationOp(type);
            Assertion.assertNotNull(relOp);
            condition = new SimpleCondition(ti.getTokValue(), ti3.getTokValue(), relOp);
        } else if (type == EXISTS) {
            condition = new SimpleCondition(ti.getTokValue(), null, RelationalOp.EXISTS);
        }
        return condition;
    }

    static RelationalOp toRelationOp(TokenType type) {
        switch (type) {
            case EQUALS:
                return RelationalOp.EQ;
            case NE:
                return RelationalOp.NE;
            case LT:
                return RelationalOp.LT;
            case GT:
                return RelationalOp.GT;
            case EXISTS:
                return RelationalOp.EXISTS;
            case LIKE:
                return RelationalOp.LIKE;
            case LTE:
                return RelationalOp.LTE;
            case GTE:
                return RelationalOp.GTE;
        }
        return null;
    }

    Transformation parseJoinStatement(Tokenizer tokenizer, Node condition) {
        Transformation transformation = new Transformation(condition);
        transformation.setJoinTransform(true);
        TokenInfo ti = tokenizer.nextToken();
        while (ti != null && ti.getType() != TO) {
            if (ti.getType() == STRING) {
                transformation.addSourceColumnName(ti.getTokValue());
            }
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && (ti.getType() == COMMA || ti.getType() == TO),
                    "',' or keyword 'to' is expected", tokenizer.getLineNo());
            if (ti.getType() == COMMA) {
                ti = tokenizer.nextToken();
            }
        }
        errorIfNot(ti != null && ti.getType() == TO, "Keyword 'to' is expected", tokenizer.getLineNo());
        if (ti.getType() == TO) {
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after the keyword 'to'",
                    tokenizer.getLineNo());
            transformation.setDestColumnName(ti.getTokValue());
        }
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && (ti.getType() == SEMICOLON || ti.getType() == APPLY) ,
                "Keyword 'to' or ';' is expected after destination path", tokenizer.getLineNo());
        if (ti.getType() == APPLY) {
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == DOUBLE_LCB, "'{{' is expected after the keyword 'apply'", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == SCRIPT, "A Python script is expected after '{{'",
                    tokenizer.getLineNo());
            transformation.setScript(ti.getTokValue());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == DOUBLE_RCB, "'}}' is expected after the Python script", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == SEMICOLON, "';' is expected after '}}'", tokenizer.getLineNo());
        }
        return transformation;
    }

    Transformation parseTransformStatement(Tokenizer tokenizer, Node condition) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && (ti.getType() == COLUMN || ti.getType() == COLUMNS || ti.getType() == UNION),
                "Needs column|columns|union after keyword transform", tokenizer.getLineNo());
        Transformation transformation = new Transformation(condition);
        if (ti.getType() == TokenType.COLUMN) {
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == STRING,
                    "A string literal is expected after column keyword", tokenizer.getLineNo());
            // System.out.println(ti);
            transformation.addSourceColumnName(ti.getTokValue());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == TO, "Keyword 'to' is expected", tokenizer.getLineNo());
        } else if (ti.getType() == COLUMNS || ti.getType() == UNION) {
            if (ti.getType() == UNION) {
                transformation.setUnionOfSourceColumns(true);
            }
            ti = tokenizer.nextToken();
            while (ti != null && ti.getType() != TO) {
                if (ti.getType() == STRING) {
                    transformation.addSourceColumnName(ti.getTokValue());
                }
                ti = tokenizer.nextToken();
                errorIfNot(ti != null && (ti.getType() == COMMA || ti.getType() == TO),
                        "',' or keyword 'to' is expected", tokenizer.getLineNo());
                if (ti.getType() == COMMA) {
                    ti = tokenizer.nextToken();
                }
            }
            errorIfNot(ti != null && ti.getType() == TO, "Keyword 'to' is expected", tokenizer.getLineNo());
        }
        if (ti.getType() == TO) {
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after the keyword 'to'",
                    tokenizer.getLineNo());
            transformation.setDestColumnName(ti.getTokValue());
        }
        ti = tokenizer.nextToken();
        Assertion.assertTrue(ti != null && (ti.getType() == SEMICOLON || ti.getType() == APPLY || ti.getType() == ASSIGN));
        if (ti.getType() == ASSIGN) {
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == NAME, "name' is expected after 'from'", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == FROM, "'from' is expected after 'assign'", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == STRING, "A string is expected after the keyword 'from'",
                    tokenizer.getLineNo());
            transformation.setAssignFromJsonPath(ti.getTokValue());
        }
        if (ti.getType() == APPLY) {
            ti = tokenizer.nextToken();
            if (ti.getType() == LITERAL) {
                // predefined or registered function
                String functionName = ti.getTokValue();
                ITransformationFunction function = this.registry.getFunction(functionName);
                Assertion.assertNotNull(function);
                ti = tokenizer.nextToken();
                Assertion.assertTrue(ti != null && ti.getType() == LEFT_PAREN);
                int idx = 1;
                do {
                    ti = tokenizer.nextToken();
                    Assertion.assertTrue(ti != null);
                    if (ti.getType() == STRING) {
                        function.addParam("param" + idx, ti.getTokValue());
                        idx++;
                    }
                    ti = tokenizer.nextToken();
                    Assertion.assertTrue(ti != null && (ti.getType() == COMMA || ti.getType() == RIGHT_PAREN));
                } while (ti != null && ti.getType() != RIGHT_PAREN);

                transformation.setTransformationFunction(function);
                ti = tokenizer.nextToken();
                errorIfNot(ti != null && ti.getType() == SEMICOLON, "';' is expected after '}}'", tokenizer.getLineNo());

            } else {

                errorIfNot(ti != null && ti.getType() == DOUBLE_LCB, "'{{' is expected after the keyword 'apply'", tokenizer.getLineNo());
                ti = tokenizer.nextToken();
                errorIfNot(ti != null && ti.getType() == SCRIPT, "A Python script is expected after '{{'",
                        tokenizer.getLineNo());
                transformation.setScript(ti.getTokValue());
                ti = tokenizer.nextToken();
                errorIfNot(ti != null && ti.getType() == DOUBLE_RCB, "'}}' is expected after the Python script", tokenizer.getLineNo());
                ti = tokenizer.nextToken();
                errorIfNot(ti != null && ti.getType() == SEMICOLON, "';' is expected after '}}'", tokenizer.getLineNo());

            }
        }

        return transformation;
    }

    enum LogicalOp {
        AND, OR, NONE
    }

    public interface Node {
        Node getLeftOperand();

        Node getRightOperand();

        LogicalOp getLogicalOp();
    }

    public enum RelationalOp {
        EQ, LT, GT, LTE, GTE, NE, EXISTS, LIKE, NOT_EXISTS, NOT_LIKE
    }

    public static class SimpleCondition implements Node {
        String lhs;
        String rhs;
        RelationalOp relOp;

        public SimpleCondition(String lhs, String rhs, RelationalOp relOp) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.relOp = relOp;
        }

        @Override
        public Node getLeftOperand() {
            return this;
        }

        @Override
        public Node getRightOperand() {
            return null;
        }

        @Override
        public LogicalOp getLogicalOp() {
            return LogicalOp.NONE;
        }

        public String getLhs() {
            return lhs;
        }

        public String getRhs() {
            return rhs;
        }

        public RelationalOp getRelOp() {
            return relOp;
        }
    }


    public static class BinaryCondition implements Node {
        Node leftOperand;
        Node rightOperand;
        LogicalOp logicalOp = LogicalOp.AND;

        public void setLeftOperand(Node leftOperand) {
            this.leftOperand = leftOperand;
        }

        public void setRightOperand(Node rightOperand) {
            this.rightOperand = rightOperand;
        }

        public void setLogicalOp(LogicalOp logicalOp) {
            this.logicalOp = logicalOp;
        }

        @Override
        public Node getLeftOperand() {
            return null;
        }

        @Override
        public Node getRightOperand() {
            return null;
        }

        @Override
        public LogicalOp getLogicalOp() {
            return null;
        }
    }

    public static class Tokenizer {
        int curIdx = 0;
        char[] charArr;
        boolean inScript = false;
        boolean inString = false;
        boolean inComment = false;
        int lineNo = 1;
        Stack<TokenInfo> tiStack = new Stack<TokenInfo>();
        Map<String, TokenType> typeMap = new HashMap<String, TokenType>();

        public Tokenizer(String transformScript) {
            charArr = transformScript.toCharArray();
            typeMap.put("transform", TRANSFORM);
            typeMap.put("union", UNION);
            typeMap.put("to", TO);
            typeMap.put("map", MAP);
            typeMap.put("column", COLUMN);
            typeMap.put("columns", COLUMNS);
            typeMap.put("apply", APPLY);
            typeMap.put("{{", DOUBLE_LCB);
            typeMap.put("}}", DOUBLE_RCB);
            typeMap.put(";", SEMICOLON);
            typeMap.put("/*", COMMENT_START);
            typeMap.put("*/", COMMENT_END);
            typeMap.put("[", LEFT_RB);
            typeMap.put("]", RIGHT_RB);
            typeMap.put(".", PERIOD);
            typeMap.put("assign", ASSIGN);
            typeMap.put("from", FROM);
            typeMap.put("name", NAME);
            typeMap.put("let", LET);
            typeMap.put("=", EQUALS);
            typeMap.put(">", GT);
            typeMap.put("<", LT);
            typeMap.put(">=", GTE);
            typeMap.put("<=", LTE);
            typeMap.put("<>", NE);
            typeMap.put("not", NOT);
            typeMap.put("exists", EXISTS);
            typeMap.put("like", LIKE);
            typeMap.put("and", AND);
            typeMap.put("or", OR);
            typeMap.put("if", IF);
            typeMap.put("then", THEN);
            typeMap.put("join", JOIN);
        }

        public void pushBack(TokenInfo ti) {
            tiStack.push(ti);
        }

        public int getLineNo() {
            return lineNo;
        }

        public TokenInfo nextToken() {
            if (!tiStack.isEmpty()) {
                return tiStack.pop();
            }
            char c;
            StringBuilder sb = new StringBuilder();
            while (curIdx < charArr.length) {
                if (inComment) {
                    while (curIdx < charArr.length) {
                        c = nextChar();
                        if (c == '*') {
                            char c1 = nextChar();
                            if (c1 == '/') {
                                inComment = false;
                                break;
                            } else {
                                // move back cursor
                                curIdx--;
                            }
                        }
                    }
                }
                c = nextChar();
                if (inScript) {
                    if (c == '}') {
                        if (curIdx < charArr.length && charArr[curIdx] == '}') {
                            TokenInfo ti = new TokenInfo("}}", TokenType.DOUBLE_RCB);
                            tiStack.push(ti);
                            curIdx++;
                            inScript = false;
                            return new TokenInfo(sb.toString().trim(), TokenType.SCRIPT);
                        } else {
                            sb.append(c);
                        }
                    } else {
                        sb.append(c);
                    }
                } else if (!Character.isWhitespace(c)) {
                    if (inString && c == '"') {
                        if (curIdx > 0 && charArr[curIdx - 1] == '\\') {
                            sb.append(c);
                        } else {
                            inString = false;
                            return new TokenInfo(sb.toString().trim(), TokenType.STRING);
                        }
                    } else if (!inScript && !inString && !inComment && (c == ';' || c == ',' || c == '{' || c == '}'
                            || c == '(' || c == ')' || c == '"' || c == '/' || c == '[' || c == ']' || c == '.')
                            || c == '<' || c == '>') {
                        if (c == '{') {
                            char c1 = nextChar();
                            Assertion.assertTrue(c1 == '{');
                            inScript = true;
                            return new TokenInfo("{{", TokenType.DOUBLE_LCB);
                        } else if (c == '}') {
                            char c1 = nextChar();
                            Assertion.assertTrue(c1 == '}');
                            return new TokenInfo("}}", TokenType.DOUBLE_RCB);
                        } else if (c == ';') {
                            return new TokenInfo(";", TokenType.SEMICOLON);
                        } else if (c == '(') {
                            if (sb.length() > 0) {
                                tiStack.push(new TokenInfo("(", TokenType.LEFT_PAREN));
                                return new TokenInfo(sb.toString().trim(), TokenType.LITERAL);
                            } else {
                                return new TokenInfo("(", TokenType.LEFT_PAREN);
                            }
                        } else if (c == ')') {
                            return new TokenInfo(")", TokenType.RIGHT_PAREN);
                        } else if (c == ',') {
                            return new TokenInfo(",", TokenType.COMMA);
                        } else if (c == '"') {
                            inString = true;
                        } else if (c == '/') {
                            char c1 = nextChar();
                            Assertion.assertTrue(c1 == '*', "Expected * after / at line " + lineNo);
                            inComment = true;
                        } else if (c == '[') {
                            if (sb.length() > 0) {
                                tiStack.push(new TokenInfo("[", TokenType.LEFT_RB));
                                return new TokenInfo(sb.toString().trim(), TokenType.LITERAL);
                            } else {
                                return new TokenInfo("[", TokenType.LEFT_RB);
                            }
                        } else if (c == ']') {
                            return new TokenInfo("]", TokenType.RIGHT_RB);
                        } else if (c == '.') {
                            return new TokenInfo(".", TokenType.PERIOD);
                        } else if (c == '>') {
                            char c1 = nextChar();
                            if (c1 == '=') {
                                return new TokenInfo(">=", TokenType.GTE);
                            } else {
                                pushBackChar();
                                return new TokenInfo(">", TokenType.GT);
                            }
                        } else if (c == '<') {
                            char c1 = nextChar();
                            if (c1 == '=') {
                                return new TokenInfo("<=", TokenType.LTE);
                            } else if (c1 == '>') {
                                return new TokenInfo("<>", TokenType.NE);
                            } else {
                                pushBackChar();
                                return new TokenInfo("<", TokenType.LT);
                            }
                        }
                    } else {
                        sb.append(c);
                    }
                } else if (Character.isWhitespace(c)) {
                    if (!inString && sb.length() > 0) {
                        String s = sb.toString();
                        TokenType tokenType = typeMap.get(s.toLowerCase());
                        if (tokenType != null) {
                            TokenInfo ti = new TokenInfo(s, tokenType);
                            sb.setLength(0);
                            eatWS();
                            return ti;
                        } else {
                            TokenInfo ti = new TokenInfo(s, TokenType.LITERAL);
                            eatWS();
                            return ti;
                        }
                    } else if (inString) {
                        sb.append(c);
                    }
                }
            }
            return null;
        }

        char nextChar() {
            if (curIdx < charArr.length) {
                char c = charArr[curIdx++];
                if (c == '\n') {
                    lineNo++;
                }
                return c;
            }
            return (char) -1;
        }

        void pushBackChar() {
            --curIdx;
        }

        void eatWS() {
            while (curIdx < charArr.length) {
                char c = charArr[curIdx];
                if (Character.isSpaceChar(c)) {
                    if (c == '\n') {
                        lineNo++;
                    }
                    curIdx++;
                } else {
                    break;
                }
            }
        }
    }

    public static class TokenInfo {
        String tokValue;
        TokenType type;

        public TokenInfo(String tokValue, TokenType type) {
            this.tokValue = tokValue;
            this.type = type;
        }

        public String getTokValue() {
            return tokValue;
        }

        public TokenType getType() {
            return type;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TokenInfo::{");
            sb.append("tokValue='").append(tokValue).append('\'');
            sb.append(", type=").append(type.toString());
            sb.append('}');
            return sb.toString();
        }
    }


    public static void executePythonScript() {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.exec("import sys");
        interpreter.exec("print sys");
        interpreter.set("orig_colName", new PyString("genotype"));
        interpreter.exec("result=orig_colName + '_1'");
        PyObject result = interpreter.get("result");
        String resultStr = result.asString();
        System.out.println("result:" + resultStr);

    }

    public static void main(String[] args) {
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();

        registry.registerFunction("toStandardDate",
                new TransformationFunctionRegistry.TransformationFunctionConfig(
                        "org.neuinfo.foundry.common.transform.ToStandardDateTransformation"));
        // executePythonScript();
        String ts = "/* some comment */ " + "transform column \"Stk #\" to \"id\" apply {{result=orig_colName + '_1'}};"
                + "transform column \"Date added\" to \"date_added\" apply toStandardDate(\"MM/dd/yyyy\");";
        ts += " transform columns \"$.'PDBx:datablock'.'PDBx:database_2Category'.'PDBx:database_2'[*].'@database_id'\",\"$.'PDBx:datablock'.'PDBx:database_2Category'.'PDBx:database_2'[*].'@database_code'\"  " + "" +
                "to \"identifiers[].Source\" apply {{ result = value2.lower() + ':' + value1}};";

        ts += "\nmap biocaddie-003[\"$.'Cell ID'\"].\"name\" to \"cell.name\";";

        ts = "if ( \"$.'PDBx:datablock'\" exists or \"$.'Cell ID'\" = \"45\" ) and \"$.x\" <> \"a\" then transform column \"Date added\" to \"date_added\" apply toStandardDate(\"MM/dd/yyyy\");";

        TransformationLanguageInterpreter interpreter = new TransformationLanguageInterpreter(registry);
        interpreter.parse(ts);


        Assertion.assertEquals(interpreter.getTransformations().size(), 1);
        // Assertion.assertEquals(interpreter.getMappings().size(), 1);

    }
}
