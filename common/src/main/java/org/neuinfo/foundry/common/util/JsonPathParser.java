package org.neuinfo.foundry.common.util;

import java.io.StreamTokenizer;
import java.io.StringReader;

/**
 * Created by bozyurt on 6/19/15.
 */
public class JsonPathParser {
    StreamTokenizer stok;

    public JsonPathParser(String jsonPathStr) {
        stok = new StreamTokenizer(new StringReader(jsonPathStr));
        stok.resetSyntax();
        stok.wordChars('a', 'z');
        stok.wordChars('A', 'Z');
        stok.wordChars('_', '_');
        stok.wordChars('0', '9');
        stok.whitespaceChars(' ', ' ');
        stok.whitespaceChars('\t', '\t');
        stok.ordinaryChar('.');
        stok.ordinaryChar('$');
        stok.ordinaryChar('*');
        stok.ordinaryChar('[');
        stok.ordinaryChar(']');
        stok.ordinaryChar('?');
        stok.ordinaryChar('@');
        stok.ordinaryChar('(');
        stok.ordinaryChar(')');
        stok.ordinaryChar('=');
        stok.quoteChar('\'');
    }

    public JSONPathProcessor.Path parse() throws Exception {
        int tokCode;
        JSONPathProcessor.Path path = null;
        while ((tokCode = stok.nextToken()) != StreamTokenizer.TT_EOF) {
            if (tokCode == '$') {
                tokCode = stok.nextToken();
                if (tokCode == '.') {
                    int tc2 = stok.nextToken();
                    if (tc2 == StreamTokenizer.TT_WORD || tc2 == '\'') {
                        String name = stok.sval;
                        int tc3 = stok.nextToken();
                        JSONPathProcessor.Node node;
                        if (tc3 == '[') {
                            node = handleIndex(name);
                            node.type = JSONPathProcessor.Node.FROM_ROOT;
                        } else {
                            stok.pushBack();
                            node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.FROM_ROOT);
                        }
                        if (path == null) {
                            path = new JSONPathProcessor.Path(node);
                        } else {
                            path.add(node);
                        }
                    } else if (tc2 == '.') {
                        tc2 = stok.nextToken();
                        checkIfWordOrString(tc2);
                        String name = stok.sval;
                        path = prepPath(path, name);
                    } else {
                        throw new Exception("Invalid syntax after $.!");
                    }
                } else if (tokCode == '[') {
                    int tc2 = stok.nextToken();
                    if (tc2 == '\'') {
                        String name = stok.sval;
                        JSONPathProcessor.Node node;
                        if (path == null) {
                            node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.FROM_ROOT);
                            path = new JSONPathProcessor.Path(node);
                        } else {
                            node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
                            path.add(node);
                        }
                        int tc3 = stok.nextToken();
                        checkIfValid(name, tc3, ']');
                    } else {
                        //TODO
                    }
                } else {
                    throw new Exception("Syntax error '.' or '[' expected after root $!");
                }
            } else if (tokCode == '.') {
                int tc2 = stok.nextToken();
                if (tc2 == StreamTokenizer.TT_WORD || tc2 == '\'') {
                    String name = stok.sval;
                    int tc3 = stok.nextToken();
                    JSONPathProcessor.Node node;
                    if (tc3 == '[') {
                        node = handleIndex(name);
                    } else {
                        stok.pushBack();
                        node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
                    }
                    path.add(node);
                } else if (tc2 == '.') {
                    tc2 = stok.nextToken();
                    checkIfWordOrString(tc2);
                    String name = stok.sval;
                    path = prepPath(path, name);
                }
            } else if (tokCode == '[') {
                int tc2 = stok.nextToken();
                if (tc2 == '\'') {
                    String name = stok.sval;
                    JSONPathProcessor.Node node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
                    path.add(node);
                    int tc3 = stok.nextToken();
                    checkIfValid(name, tc3, ']');
                }
            }
        }
        return path;
    }

    private void checkIfWordOrString(int tc) throws Exception {
        if (tc != StreamTokenizer.TT_WORD && tc != '\'') {
            throw new Exception("A word of string is expected but found " + (char) tc);
        }
    }

    private JSONPathProcessor.Path prepPath(JSONPathProcessor.Path path, String name) throws Exception {
        int tc3 = stok.nextToken();
        JSONPathProcessor.Node node;
        if (tc3 == '[') {
            node = handleIndex(name);
            node.type = JSONPathProcessor.Node.FROM_ANYWHERE;
        } else {
            stok.pushBack();
            node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.FROM_ANYWHERE);
        }
        if (path == null) {
            path = new JSONPathProcessor.Path(node);
        } else {
            path.add(node);
        }
        return path;
    }

    private void checkIfValid(String name, int tc, int expected) throws Exception {
        if (tc == StreamTokenizer.TT_EOF) {
            throw new Exception("Unexpected end of expression after " + name);
        }
        if (tc != expected) {
            throw new Exception("Expected '" + (char) expected + " after " + name);
        }
    }

    private JSONPathProcessor.Node handleIndex(String name) throws Exception {
        final int tc = stok.nextToken();
        if (tc == '*') {
            int tc2 = stok.nextToken();
            checkIfValid(name, tc2, ']');
            JSONPathProcessor.Node node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
            node.includeAll = true;
            return node;
        } else if (tc == StreamTokenizer.TT_WORD) {
            int index = Integer.parseInt(stok.sval);
            JSONPathProcessor.Node node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
            node.idx = index;
            int tc2 = stok.nextToken();
            checkIfValid(name, tc2, ']');
            return node;
        } else if (tc == '?') {
            int tc3 = stok.nextToken();
            checkIfValid("?", tc3, '(');
            int tc4 = stok.nextToken();
            checkIfValid("(", tc4, '@');
            tc4 = stok.nextToken();
            checkIfValid("@", tc4, '.');

            int tc5 = stok.nextToken();
            if (tc5 == '\'') {
                JSONPathProcessor.Node node = new JSONPathProcessor.Node(name, JSONPathProcessor.Node.INNER_NODE);
                String predicateName = stok.sval;
                tc5 = stok.nextToken();
                checkIfValid("attribute " + predicateName, tc5, '=');
                tc5 = stok.nextToken();
                checkIfValid("=", tc5, '\'');
                String predicateTest = stok.sval;
                node.predicateType = JSONPathProcessor.Node.EQUALS;
                node.predicateName = predicateName;
                node.predicateTest = predicateTest;
                tc5 = stok.nextToken();
                checkIfValid(predicateTest, tc5, ')');
                tc5 = stok.nextToken();
                checkIfValid(")", tc5, ']');
                return node;
            } else {
                throw new Exception("A single quoted field name is expected after @");
            }
        }


        return null;
    }
}
