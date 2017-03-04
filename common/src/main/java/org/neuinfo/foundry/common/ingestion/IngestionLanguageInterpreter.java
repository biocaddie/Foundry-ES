package org.neuinfo.foundry.common.ingestion;

import org.apache.log4j.Logger;

import java.util.*;

import static org.neuinfo.foundry.common.ingestion.TokenType.*;

/**
 * Created by bozyurt on 2/22/17.
 * <p/>
 * DOWNLOAD {path} AS {alias} [FORMAT {format_type}];
 * EXTRACT  {format} {path} AS {alias} FROM {alias};
 * PARTITION {alias} BY {line| RE regex|jsonPath} [START {elementName|jsonPath}];
 * SET {param} = {value-string} FOR {alias};
 * JOIN {alias-list} BY {join-expr} AS {alias};
 * INGEST {alias};
 */
public class IngestionLanguageInterpreter {
    List<IngestCommandInfo> commandInfos = new ArrayList<IngestCommandInfo>(10);
    Map<String, IngestCommandInfo> cmdMap = new HashMap<String, IngestCommandInfo>();
    static Map<String, TokenType> typeMap = new HashMap<String, TokenType>();
    private final static Logger log = Logger.getLogger(IngestionLanguageInterpreter.class);

    static {
        typeMap.put("download", DOWNLOAD);
        typeMap.put("extract", EXTRACT);
        typeMap.put("partition", PARTITION);
        typeMap.put("join", JOIN);
        typeMap.put("as", AS);
        typeMap.put("by", BY);
        typeMap.put("from", FROM);
        typeMap.put("with", WITH);
        typeMap.put("format", FORMAT);
        typeMap.put("start", START);
        typeMap.put("for", FOR);
        typeMap.put("set", SET);
        typeMap.put("ingest", INGEST);
        typeMap.put("=", EQUAL);
        typeMap.put(";", SEMICOLON);
        typeMap.put(",", COMMA);
        typeMap.put("/*", COMMENT_START);
        typeMap.put("*/", COMMENT_END);
    }

    public void parse(String ingestionScript) {
        Tokenizer tokenizer = new Tokenizer(ingestionScript);
        TokenInfo ti;
        while ((ti = tokenizer.nextToken()) != null) {
            log.info(ti);
            IngestCommandInfo ici;
            switch (ti.getType()) {
                case DOWNLOAD:
                    ici = parseDownloadStatement(tokenizer);
                    commandInfos.add(ici);
                    cmdMap.put(ici.getAlias(), ici);
                    break;
                case EXTRACT:
                    ici = parseExtractStatement(tokenizer);
                    commandInfos.add(ici);
                    cmdMap.put(ici.getAlias(), ici);
                    break;
                case PARTITION:
                    parsePartitionStatement(tokenizer);
                    break;
                case SET:
                    parseSetStatement(tokenizer);
                    break;
                case JOIN:
                    ici = parseJoinStatement(tokenizer);
                    commandInfos.add(ici);
                    cmdMap.put(ici.getAlias(), ici);
                    break;
                case INGEST:
                    ici = parseIngestStatement(tokenizer);
                    commandInfos.add(ici);
                    cmdMap.put("ingest", ici);
                    break;
            }
        }
    }

    public List<IngestCommandInfo> getCommandInfos() {
        return commandInfos;
    }

    IngestCommandInfo parseExtractStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == LITERAL,
                "Expected a file type (e.g. csv,xml or json) after EXTRACT", tokenizer.getLineNo());
        IngestCommandInfo ici = new IngestCommandInfo("extract");
        String format = ti.getTokValue();

        boolean knownFormat = format.equalsIgnoreCase("csv") || format.equalsIgnoreCase("xml")
                || format.equalsIgnoreCase("json");
        errorIfNot(knownFormat, "Not a supported file type:" + format, tokenizer.getLineNo());
        FileType ft = FileType.valueOf(format.toUpperCase());
        ici.setFileType(ft);
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "Expected a path string for the file to be extracted after file type", tokenizer.getLineNo());
        ici.setPath(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == AS,
                "expected AS after extract file path", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == LITERAL,
                "A unique alias is expected after AS for extract command", tokenizer.getLineNo());
        ici.setAlias(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == FROM,
                "expected FROM after extract file path alias", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        boolean defined = false;
        if (ti != null && ti.getType() == LITERAL) {
            defined = cmdMap.containsKey(ti.getTokValue());
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && defined,
                "An already defined alias is expected after FROM", tokenizer.getLineNo());
        ici.setFromAlias(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
        return ici;
    }

    void parsePartitionStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        IngestCommandInfo ici = null;

        if (ti != null && ti.getType() == LITERAL) {
            String alias = ti.getTokValue();
            ici = cmdMap.get(alias);
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && ici != null,
                "An already defined alias is expected after PARTITION", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == BY,
                "expected BY after alias for PARTITION command", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING || ti.getType() == LITERAL,
                "Expected document json path/element name as string or literal line after BY", tokenizer.getLineNo());
        if (ti.getType() == STRING) {
            ici.setDocElement(ti.getTokValue());
        } else {
            if (ti.getTokValue().equalsIgnoreCase("line")) {
                ici.setLinePerRecord(true);
            }
        }
        ti = tokenizer.nextToken();
        if (ti != null && ti.getType() == START) {
            errorIfNot(ti != null && ti.getType() == START,
                    "expected START after document element", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            errorIfNot(ti != null && ti.getType() == STRING,
                    "Expected top json path/element name as string after START", tokenizer.getLineNo());
            ici.setTopElement(ti.getTokValue());
            ti = tokenizer.nextToken();
        }
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
    }


    void parseSetStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "Expected parameter name after SET", tokenizer.getLineNo());
        String paramName = ti.getTokValue();
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == EQUAL,
                "Expected '=' after parameter name", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "Expected parameter value after '='", tokenizer.getLineNo());
        String paramValue = ti.getTokValue();
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == FOR,
                "Expected keyword FOR after parameter value", tokenizer.getLineNo());

        ti = tokenizer.nextToken();
        IngestCommandInfo ici = null;
        if (ti != null && ti.getType() == LITERAL) {
            String alias = ti.getTokValue();
            ici = cmdMap.get(alias);
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && ici != null,
                "An already defined alias is expected after FOR", tokenizer.getLineNo());
        ici.getParamMap().put(paramName, paramValue);
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
    }

    IngestCommandInfo parseIngestStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        IngestCommandInfo ici = new IngestCommandInfo("ingest");
        IngestCommandInfo ingestICI = null;
        if (ti != null && ti.getType() == LITERAL) {
            ingestICI = cmdMap.get(ti.getTokValue());
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && ingestICI != null,
                "Expected an already defined alias for INGEST", tokenizer.getLineNo());
        ici.setAlias(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
        return ici;
    }

    IngestCommandInfo parseJoinStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        IngestCommandInfo leftICI = null;
        IngestCommandInfo rightICI = null;
        IngestCommandInfo ici = new IngestCommandInfo("join");
        if (ti != null && ti.getType() == LITERAL) {
            leftICI = cmdMap.get(ti.getTokValue());
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && leftICI != null,
                "Expected an already defined alias for JOIN", tokenizer.getLineNo());
        ici.setLeft(leftICI);
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == COMMA,
                "expected ',' after left alias in JOIN", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        if (ti != null && ti.getType() == LITERAL) {
            rightICI = cmdMap.get(ti.getTokValue());
        }
        errorIfNot(ti != null && ti.getType() == LITERAL && rightICI != null,
                "Expected an already defined alias for JOIN", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == BY,
                "expected BY after the second join alias", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "Expected join expression string after BY", tokenizer.getLineNo());
        ici.setJoinStr(ti.getTokValue());

        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == AS,
                "expected AS after download path", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == LITERAL,
                "A unique alias is expected after AS for JOIN", tokenizer.getLineNo());
        ici.setAlias(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
        return ici;
    }

    IngestCommandInfo parseDownloadStatement(Tokenizer tokenizer) {
        TokenInfo ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == STRING,
                "Expected a path as string after download", tokenizer.getLineNo());
        IngestCommandInfo ici = new IngestCommandInfo("download");
        ici.setPath(ti.getTokValue());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == AS,
                "expected AS after download path", tokenizer.getLineNo());
        ti = tokenizer.nextToken();
        errorIfNot(ti != null && ti.getType() == LITERAL,
                "A unique alias is expected after AS for download command", tokenizer.getLineNo());
        ici.setAlias(ti.getTokValue());


        ti = tokenizer.nextToken();
        if (ti != null && ti.getType() == FORMAT) {
            errorIfNot(ti != null && ti.getType() == FORMAT,
                    "expected FORMAT after the download alias", tokenizer.getLineNo());
            ti = tokenizer.nextToken();

            errorIfNot(ti != null && ti.getType() == LITERAL,
                    "A recognized file format is expected after FORMAT for download command", tokenizer.getLineNo());
            FormatType ft = FormatType.valueOf(ti.getTokValue().toUpperCase());

            ici.setFormatType(ft);
            ti = tokenizer.nextToken();
        }
        errorIfNot(ti != null && ti.getType() == SEMICOLON, "A ';' is expected", tokenizer.getLineNo());
        return ici;
    }

    public static void errorIfNot(boolean expressionVal, String msg, int lineNo) {
        if (!expressionVal) {
            throw new org.neuinfo.foundry.common.transform.SyntaxException(String.format("%s at line %d!", msg, lineNo));
        }
    }

    public static class Tokenizer {
        int curIdx = 0;
        char[] charArr;
        boolean inString = false;
        boolean inComment = false;
        int lineNo = 1;
        Stack<TokenInfo> tiStack = new Stack<TokenInfo>();

        public Tokenizer(String ingestionScript) {
            this.charArr = ingestionScript.toCharArray();

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
                if (!Character.isWhitespace(c)) {
                    if (inString && c == '"') {
                        if (curIdx > 0 && charArr[curIdx - 1] == '\\') {
                            sb.append(c);
                        } else {
                            inString = false;
                            return new TokenInfo(sb.toString().trim(), TokenType.STRING);
                        }
                    } else if (!inString && !inComment && (c == ';' || c == '"' || c == ',' || c == '=')) {
                        if (c == ';') {
                            if (sb.length() > 0) {
                                TokenInfo ti = prepTokenInfo(sb, false);
                                sb.setLength(0);
                                tiStack.push(new TokenInfo(";", TokenType.SEMICOLON));
                                return ti;
                            }
                            return new TokenInfo(";", TokenType.SEMICOLON);
                        } else if (c == ',') {
                            if (sb.length() > 0) {
                                TokenInfo ti = prepTokenInfo(sb, false);
                                sb.setLength(0);
                                tiStack.push(new TokenInfo(",", TokenType.COMMA));
                                return ti;
                            }
                            return new TokenInfo(",", TokenType.COMMA);
                        } else if (c == '=') {
                            if (sb.length() > 0) {
                                TokenInfo ti = prepTokenInfo(sb, false);
                                sb.setLength(0);
                                tiStack.push(new TokenInfo("=", TokenType.EQUAL));
                                return ti;
                            }
                            return new TokenInfo("=", TokenType.EQUAL);
                        } else {
                            inString = true;
                        }
                    } else {
                        sb.append(c);
                    }
                } else if (Character.isWhitespace(c)) {
                    if (inString) {
                        sb.append(c);
                    } else if (sb.length() > 0) {
                        return prepTokenInfo(sb, true);
                    }
                }
            }
            return null;
        }

        public TokenInfo prepTokenInfo(StringBuilder sb, boolean eatWhitespaces) {
            String s = sb.toString();
            TokenType tokenType = typeMap.get(s.toLowerCase());
            if (tokenType != null) {
                TokenInfo ti = new TokenInfo(s, tokenType);
                sb.setLength(0);
                if (eatWhitespaces) {
                    eatWS();
                }
                return ti;
            } else {
                TokenInfo ti = new TokenInfo(s, TokenType.LITERAL);
                if (eatWhitespaces) {
                    eatWS();
                }
                return ti;
            }
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
        final String tokValue;
        final TokenType type;

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
            sb.append(", type=").append(type);
            sb.append('}');
            return sb.toString();
        }
    }


    public static void main(String[] args) {
        String script = "download \"https://github.com/neurosynth/neurosynth-data/raw/master/current_data.tar.gz\" as a;\n" +
                "extract csv \"database.txt\" as b from a;\n" +
                "extract csv \"features.txt\" as c from a;\n" +
                "partition b by line;\n" +
                "partition c by line;\n" +
                "set \"p1\" = \"value1\" for b;\n" +
                "join b,c by \"b::$.PMID = c::$.PMID\" as d;\n" +
                "ingest d;";

        IngestionLanguageInterpreter interpreter = new IngestionLanguageInterpreter();

        interpreter.parse(script);
    }
}
