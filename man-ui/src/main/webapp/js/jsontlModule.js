var foundry = foundry || {};

foundry.JSONTLParsingModule = function () {

    var TokenType = {
        TRANSFORM: 1, COLUMN: 2, COLUMNS: 3, TO: 4, APPLY: 5, COMMA: 5,
        DOUBLE_LCB: 6, DOUBLE_RCB: 7, SEMICOLON: 8, SCRIPT: 9, LITERAL: 10,
        STRING: 11, LEFT_PAREN: 12, RIGHT_PAREN: 13, MAP: 14,
        COMMENT_START: 15, COMMENT_END: 16, LEFT_RB: 17, RIGHT_RB: 18, PERIOD: 19,
        ASSIGN: 20, NAME: 21, FROM: 22, UNION: 23, LET: 24, EQUALS: 25,
        GT: 26, LT: 27, GTE: 28, LTE: 29, NE: 30, NOT: 31, EXISTS: 32,
        LIKE: 33, AND: 34, OR: 35, IF: 36, THEN: 37
    };
    var RelationalOpType = {
        EQ: 1, LT: 2, GT: 3, LTE: 4, GTE: 5, NE: 6, EXISTS: 7, LIKE: 8, NOT_EXISTS: 9, NOT_LIKE: 10
    };
    var LogicalOpType = {
        AND: 1, OR: 2, NONE: 0
    };

    function JSONTLParser() {
        this.transformations = [];
    }

    JSONTLParser.prototype = {
        errorIfNot: function (expressionVal, msg, lineNo) {
            if (!expressionVal) {
                throw msg + " at line " + lineNo + "!";
            }
        },
        parseTransformStatement: function (tokenizer, condition) {
            var ti, transformation = {
                sourceColumnNames: [], destColumnName: '',
                script: '', assignFromJsonPath: '', constantValue: '',
                unionOfSourceColumns: false, constantTransform: false, trFunction: false,
                condition: condition
            };
            var funcName, funcStr, TT = TokenType;
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && (ti.type == TT.COLUMN || ti.type == TT.COLUMNS ||
                ti.type == TT.UNION),
                "Needs column|columns|union after keyword transform", tokenizer.getLineNo());
            if (ti.type == TT.COLUMN) {
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.STRING,
                    "A string literal is expected after column keyword", tokenizer.getLineNo());
                transformation.sourceColumnNames.push(ti.tokValue);
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.TO, "Keyword 'to' is expected", tokenizer.getLineNo());
            } else if (ti.type == TT.COLUMNS || ti.type == TT.UNION) {
                transformation.unionOfSourceColumns = ti.type == TT.UNION;
                ti = tokenizer.nextToken();
                while (ti && ti.type != TT.TO) {
                    if (ti.type == TT.STRING) {
                        transformation.sourceColumnNames.push(ti.tokValue);
                    }
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && (ti.type == TT.COMMA || ti.type == TT.TO),
                        "',' or keyword 'to' is expected", tokenizer.getLineNo());
                    if (ti.type == TT.COMMA) {
                        ti = tokenizer.nextToken();
                    }
                }
                this.errorIfNot(ti && ti.type == TT.TO, "Keyword 'to' is expected", tokenizer.getLineNo());
            }

            if (ti.type == TT.TO) {
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.STRING,
                    "A string literal is expected after column keyword", tokenizer.getLineNo());
                transformation.destColumnName = ti.tokValue;
            }
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && (ti.type == TT.SEMICOLON || ti.type == TT.APPLY ||
                ti.type == TT.ASSIGN),
                "The destination path string can be only followed by the keywords 'apply' or 'assign' or a ';'!",
                tokenizer.getLineNo());
            if (ti.type == TT.ASSIGN) {
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.NAME, "name' is expected after 'from'", tokenizer.getLineNo());
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.FROM, "'from' is expected after 'assign'", tokenizer.getLineNo());
                ti = tokenizer.nextToken();
                this.errorIfNot(ti && ti.type == TT.STRING, "A string is expected after the keyword 'from'",
                    tokenizer.getLineNo());
                transformation.assignFromJsonPath = ti.tokValue;
            }
            if (ti.type == TT.APPLY) {
                ti = tokenizer.nextToken();
                if (ti.type == TT.LITERAL) {
                    // predefined or registered function
                    funcStr = ti.tokValue;
                    funcName = ti.tokValue;
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && ti.type == TT.LEFT_PAREN, "A '(' is expected after function name " +
                        funcName, tokenizer.getLineNo());
                    funcStr += ti.tokValue;
                    do {
                        ti = tokenizer.nextToken();
                        if (ti.type == TT.STRING) {
                            funcStr += '"' + ti.tokValue + '"';
                        }
                        ti = tokenizer.nextToken();
                        this.errorIfNot(ti && (ti.type == TT.COMMA || ti.type == TT.RIGHT_PAREN),
                            "Either a comma or ')' expected after a function argument!", tokenizer.getLineNo());
                        funcStr += ti.tokValue;
                    } while (ti && ti.type != TT.RIGHT_PAREN);
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && ti.type == TT.SEMICOLON, "A ';' is expected after a function!",
                        tokenizer.getLineNo());
                    transformation.trFunction = funcStr;
                } else {
                    this.errorIfNot(ti && ti.type == TT.DOUBLE_LCB, "'{{' is expected after the keyword 'apply'", tokenizer.getLineNo());
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && ti.type == TT.SCRIPT, "A Python script is expected after '{{'",
                        tokenizer.getLineNo());
                    transformation.script = ti.tokValue;
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && ti.type == TT.DOUBLE_RCB, "'}}' is expected after the Python script", tokenizer.getLineNo());
                    ti = tokenizer.nextToken();
                    this.errorIfNot(ti && ti.type == TT.SEMICOLON, "';' is expected after '}}'", tokenizer.getLineNo());
                }
            }
            return transformation;
        },
        parseAssignmentStatement: function (tokenizer, condition) {
            var ti, transformation = {
                sourceColumnNames: [], destColumnName: '',
                script: '', assignFromJsonPath: '', constantValue: '',
                unionOfSourceColumns: false, constantTransform: true,
                condition: condition
            };
            var TT = TokenType;
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && ti.type == TT.STRING,
                "A string literal is expected after the 'let' keyword", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && ti.type == TT.EQUALS,
                "'=' is expected after string literal", tokenizer.getLineNo());
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && ti.type == TT.STRING,
                "A string literal is expected after '='", tokenizer.getLineNo());
            transformation.constantValue = ti.tokValue;
            ti = tokenizer.nextToken();
            this.errorIfNot(ti && ti.type == TT.SEMICOLON,
                "A ';' is expected to end the let statement!", tokenizer.getLineNo());
            return transformation;
        },
        parseConditionalStatement: function (tokenizer) {
            var condition = null, ti;
            var TT = TokenType;
            condition = this.parseCondition(tokenizer);
            ti = tokenizer.nextToken();
            if (ti.type == TT.TRANSFORM) {
                return this.parseTransformStatement(tokenizer, condition);
            } else if (ti.type == TT.LET) {
                return this.parseAssignmentStatement(tokenizer, condition)
            }
        },
        parseCondition: function parseCondition(tokenizer) {
            var condition, nextTI, ti2, ti = tokenizer.nextToken();
            var TT = TokenType, leftOperand, rightOperand, bc;
            if (ti.type == TT.LEFT_PAREN) {
                condition = parseCondition(tokenizer);
                nextTI = tokenizer.nextToken();
                this.errorIfNot(nextTI != null && nextTI.type == TT.RIGHT_PAREN,
                    "A closing parenthesis is expected in the if statement", tokenizer.getLineNo());
                nextTI = tokenizer.nextToken();
                if (nextTI && nextTI.type != TT.THEN) {
                    if (nextTI.type == TT.AND || nextTI.type == TT.OR) {
                        leftOperand = condition;
                        rightOperand = parseCondition(tokenizer);
                        bc = new BinaryCondition();
                        bc.leftOperand = leftOperand;
                        bc.rightOperand = rightOperand;
                        bc.logicalOp = nextTI.type == TT.AND ? LogicalOpType.AND : LogicalOpType.OR;
                        return bc;
                    } else {
                        this.errorIfNot(false, "An unexpected token in the conditional statement", tokenizer.getLineNo());
                    }
                } else {
                    return condition;
                }
            } else {
                tokenizer.pushBack(ti);
                leftOperand = this.parseSimpleCondition(tokenizer);
                ti2 = tokenizer.nextToken();
                this.errorIfNot(ti2 && (ti2.type == TT.AND || ti2.type == TT.OR || ti2.type == TT.THEN
                    || ti2.type == TT.RIGHT_PAREN),
                    "One of 'and', 'or', 'then' or ')' is expected after a condition", tokenizer.getLineNo());
                if (ti2.type == TT.THEN) {
                    return leftOperand;
                } else if (ti2.type == TT.RIGHT_PAREN) {
                    tokenizer.pushBack(ti2);
                    return leftOperand;
                } else {
                    rightOperand = parseCondition(tokenizer);
                    condition = new BinaryCondition();
                    condition.leftOperand = leftOperand;
                    condition.rightOperand = rightOperand;
                    condition.logicalOp = ti2.type == TT.AND ? LogicalOpType.AND : LogicalOpType.OR;
                    return condition;
                }
            }
        },
        toRelationOp: function (type) {
            var TT = TokenType;
            switch (type) {
                case TT.EQUALS:
                    return RelationalOpType.EQ;
                case TT.NE:
                    return RelationalOpType.NE;
                case TT.LT:
                    return RelationalOpType.LT;
                case TT.GT:
                    return RelationalOpType.GT;
                case TT.EXISTS:
                    return RelationalOpType.EXISTS;
                case TT.LIKE:
                    TT.return
                    RelationalOpType.LIKE;
                case TT.LTE:
                    return RelationalOpType.LTE;
                case TT.GTE:
                    return RelationalOpType.GTE;
            }
            return null;
        },
        parseSimpleCondition: function (tokenizer) {
            var condition, ti2, ti3, nextTi, type, ti = tokenizer.nextToken();
            var TT = TokenType, condition = null, relOp;
            this.errorIfNot(ti && ti.type == TT.STRING,
                "Needs a string as the left hand side of a condition", tokenizer.getLineNo());
            ti2 = tokenizer.nextToken();
            type = ti2 ? ti2.type : null;
            this.errorIfNot(ti2 && (type == TT.EQUALS || type == TT.LT || type == TT.GT || type == TT.GTE ||
                type == TT.LTE || type == TT.NE || type == TT.EXISTS || type == TT.LIKE || type == TT.NOT),
                "Needs a relational operator after the left hand side of a condition", tokenizer.getLineNo());
            if (type == TT.NOT) {
                nextTi = tokenizer.nextToken();
                this.errorIfNot(nextTi && (nextTi.type == TT.EXISTS || nextTi.type == TT.LIKE),
                    "Needs either 'exists' or 'like' after 'not' in a condition", tokenizer.getLineNo());
                if (type == TT.EXISTS) {
                    condition = new SimpleCondition(ti.tokValue, null, RelationalOpType.NOT_EXISTS);
                } else {
                    ti3 = tokenizer.nextToken();
                    this.errorIfNot(ti3 != null && ti3.type == TT.STRING,
                        "Needs a string as the right hand side of a condition", tokenizer.getLineNo());
                    condition = new SimpleCondition(ti.tokValue, ti3.tokValue, RelationalOpType.NOT_LIKE);
                }
            } else if (type != TT.EXISTS) {
                ti3 = tokenizer.nextToken();
                this.errorIfNot(ti3 && ti.type == TT.STRING,
                    "Needs a string as the right hand side of a condition", tokenizer.getLineNo());

                relOp = this.toRelationOp(type);
                condition = new SimpleCondition(ti.tokValue, ti3.tokValue, relOp);
            } else if (type == TT.EXISTS) {
                condition = new SimpleCondition(ti.tokValue, null, RelationalOpType.EXISTS);
            }
            return condition;
        },
        parse: function (transformScript) {
            var ti, tokenizer = new Tokenizer(transformScript);
            var tr;
            while ((ti = tokenizer.nextToken())) {
                switch (ti.type) {
                    case TokenType.TRANSFORM:
                        tr = this.parseTransformStatement(tokenizer, null);
                        this.transformations.push(tr);
                        break;
                    case TokenType.LET:
                        tr = this.parseAssignmentStatement(tokenizer, null);
                        this.transformations.push(tr);
                        break;
                    case TokenType.IF:
                        tr = this.parseConditionalStatement(tokenizer);
                        this.transformations.push(tr);
                        break;
                }
            }
        },
        getTransformations: function () {
            return this.transformations;
        }
    };

    function SimpleCondition(lhs, rhs, relOp) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.relOp = relOp;
    }

    SimpleCondition.prototype = {
        getLeftOperand: function () {
            return this
        },
        getRightOperand: function () {
            return null
        },
        getLogicalOp: function () {
            return LogicalOpType.NONE;
        }
    };

    function BinaryCondition() {
        this.leftOperand = null;
        this.rightOperand = null;
        this.logicalOp = LogicalOpType.AND;
    }

    BinaryCondition.prototype = {
        getLeftOperand: function () {
            return this.leftOperand;
        },
        getRightOperand: function () {
            return this.rightOperand;
        },
        getLogicalOp: function () {
            return this.logicalOp;
        }
    };


    function Tokenizer(transformScript) {
        this.charArr = transformScript.split('');
        this.curIdx = 0;
        this.inScript = false;
        this.inString = false;
        this.inComment = false;
        this.lineNo = 1;
        this.tiStack = [];
        var TT = TokenType;
        this.typeMap = {
            "transform": TT.TRANSFORM, "union": TT.UNION, "to": TT.TO,
            "map": TT.MAP, "column": TT.COLUMN, "columns": TT.COLUMNS,
            "apply": TT.APPLY, "{{": TT.DOUBLE_LCB, "}}": TT.DOUBLE_RCB,
            ";": TT.SEMICOLON, "/*": TT.COMMENT_START, "*/": TT.COMMENT_END,
            "[": TT.LEFT_RB, "]": TT.RIGHT_RB, ".": TT.PERIOD,
            "assign": TT.ASSIGN, "name": TT.NAME, "let": TT.LET, "=": TT.EQUALS,
            ">": TT.GT, "<": TT.LT, ">=": TT.GTE, "<=": TT.LTE, "<>": TT.NE,
            "not": TT.NOT, "exists": TT.EXISTS, "like": TT.LIKE, "and": TT.AND,
            "or": TT.OR, "if": TT.IF, "then": TT.THEN
        };
    }

    Tokenizer.prototype = {
        pushBack: function (ti) {
            this.tiStack.push(ti);
        },
        nextChar: function () {
            if (this.curIdx < this.charArr.length) {
                return this.charArr[this.curIdx++];
            }
            return -1;
        },
        pushBackChar: function () {
            this.curIdx--;
        },
        getLineNo: function () {
            return this.lineNo;
        },
        isWhitespace: function (c) {
            return c == ' ' || c == '\n' || c == '\t' || c == '\r';
        },
        eatWS: function () {
            var c;
            while (this.curIdx < this.charArr.length) {
                c = this.charArr[this.curIdx];
                if (c == ' ' || c == '\n' || c == '\t' || c == '\r') {
                    if (c == '\n') {
                        this.lineNo++;
                    }
                    this.curIdx++;
                } else {
                    break;
                }
            }
        },
        nextToken: function () {
            if (this.tiStack.length > 0) {
                return this.tiStack.pop();
            }
            var c, c1, sb = '', ti, len = this.charArr.length;
            while (this.curIdx < len) {
                if (this.inComment) {
                    while (this.curIdx < len) {
                        c = this.nextChar();
                        if (c == '*') {
                            c1 = this.nextChar();
                            if (c1 == '/') {
                                this.inComment = false;
                                break;
                            } else {
                                // move back cursor
                                this.curIdx--;
                            }
                        }
                    }
                }
                c = this.nextChar();
                if (this.inScript) {
                    if (c == '}') {
                        if (this.curIdx < len && this.charArr[this.curIdx] == '}') {
                            ti = {tokValue: '}}', type: TokenType.DOUBLE_RCB};
                            this.tiStack.push(ti);
                            this.curIdx++;
                            this.inScript = false;
                            return {tokValue: $.trim(sb), type: TokenType.SCRIPT};
                        } else {
                            sb += String(c);
                        }
                    } else {
                        sb += String(c);
                    }
                } else if (!this.isWhitespace(c)) {
                    if (this.inString && c == '"') {
                        if (this.curIdx > 0 && this.charArr[this.curIdx - 1] == '\\') {
                            sb += String(c);
                        } else {
                            this.inString = false;
                            return {tokValue: $.trim(sb), type: TokenType.STRING};
                        }
                    } else if (!this.inScript && !this.inString && !this.inComment &&
                        (c == ';' || c == ',' || c == '{' || c == '}' || c == '('
                        || c == ')' || c == '"' || c == '/' || c == '['
                        || c == ']' || c == '.') || c == '<' || c == '>') {
                        if (c == '{') {
                            c1 = this.nextChar();
                            // assert c1 == '{'
                            this.inScript = true;
                            return {tokValue: '{{', type: TokenType.DOUBLE_LCB};
                        } else if (c == '}') {
                            c1 = this.nextChar();
                            // assert c1 == '}'
                            return {tokValue: '}}', type: TokenType.DOUBLE_RCB};
                        } else if (c == ';') {
                            return {tokValue: ';', type: TokenType.SEMICOLON};
                        } else if (c == '(') {
                            if (sb.length > 0) {
                                this.tiStack.push({tokValue: '(', type: TokenType.LEFT_PAREN});
                                return {tokValue: $.trim(sb), type: TokenType.LITERAL};
                            } else {
                                return {tokValue: '(', type: TokenType.LEFT_PAREN};
                            }
                        } else if (c == ')') {
                            return {tokValue: ')', type: TokenType.RIGHT_PAREN};
                        } else if (c == ',') {
                            return {tokValue: ',', type: TokenType.COMMA};
                        } else if (c == '"') {
                            this.inString = true;
                        } else if (c == '/') {
                            c1 = this.nextChar();
                            // assert c1 == '*'
                            this.inComment = true;
                        } else if (c == '[') {
                            if (sb.length > 0) {
                                this.tiStack.push({tokValue: '[', type: TokenType.LEFT_RB});
                                return {tokValue: $.trim(sb), type: TokenType.LITERAL};
                            } else {
                                return {tokValue: '[', type: TokenType.LEFT_RB};
                            }
                        } else if (c == ']') {
                            return {tokValue: ']', type: TokenType.RIGHT_RB};
                        } else if (c == '.') {
                            return {tokValue: '.', type: TokenType.PERIOD};
                        } else if (c == '>') {
                            c1 = this.nextChar();
                            if (c1 == '=') {
                                return {tokValue: '>=', type: TokenType.GTE};
                            } else {
                                this.pushBackChar();
                                return {tokValue: '>', type: TokenType.GT};
                            }
                        } else if (c == '<') {
                            c1 = this.nextChar();
                            if (c1 == '=') {
                                return {tokValue: '<=', type: TokenType.LTE};
                            } else {
                                this.pushBackChar();
                                return {tokValue: '<', type: TokenType.LT};
                            }
                        }
                    } else {
                        sb += String(c);
                    }
                } else { // whitespace
                    if (!this.inString && sb.length > 0) {
                        var tokType = this.typeMap[$.trim(sb).toLowerCase()];
                        if (tokType) {
                            ti = {tokValue: $.trim(sb), type: tokType};
                            sb = '';
                            this.eatWS();
                            return ti;
                        } else {
                            ti = {tokValue: $.trim(sb), type: TokenType.LITERAL};
                            this.eatWS();
                            return ti;
                        }
                    } else if (this.inString) {
                        sb += String(c);
                    }
                }
            } // while
            return null;
        }
    }

    function tokenize(transformationScript) {
        var ti, tokenizer = new Tokenizer(transformationScript);
        while ((ti = tokenizer.nextToken())) {
            console.log("{tok:" + ti.tokValue + ", type:" + ti.type + "}");
        }
    }

    function parse(transformationScript) {
        var parser = new JSONTLParser();
        parser.parse(transformationScript);
        var i, trArr = parser.getTransformations();
        for (i = 0; i < trArr.length; i++) {
            console.dir(trArr[i]);
        }
        return trArr;
    }

    function parseRule(transformationScript) {
        var parser = new JSONTLParser();
        parser.parse(transformationScript);
        var trArr = parser.getTransformations();
        if (trArr) {
            return trArr[0];
        }
        return null;
    }

    return {
        tokenize: tokenize,
        parse: parse,
        parseRule: parseRule
    };
}();
