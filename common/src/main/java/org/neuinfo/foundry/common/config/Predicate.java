package org.neuinfo.foundry.common.config;

import org.jdom2.Element;
import org.json.JSONObject;

/**
 * Created by bozyurt on 4/24/14.
 */
public class Predicate {
    private String name;
    private String value;
    private Operator op;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Predicate{");
        sb.append("name='").append(name).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append(", op=").append(op);
        sb.append('}');
        return sb.toString();
    }

    public Predicate(String name, String value, Operator op) {
        this.name = name;
        this.value = value;
        this.op = op;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public Operator getOp() {
        return op;
    }

    boolean satisfied(String variableValue) {
       if (op == Operator.EQ) {
           return value.equals(variableValue);
       } else if (op == Operator.NE) {
           return !value.equals(variableValue);
       } else if (op == Operator.STARTS_WITH) {
           return variableValue.startsWith(value);
       }
       return false;
    }

    public static Predicate fromXml(Element elem) {
        String name = elem.getAttributeValue("op");
        Operator op = Operator.STARTS_WITH;
        if (name.equals(Operator.EQ.getLabel())) {
            op = Operator.EQ;
        } else if (name.equals(Operator.NE.getLabel())) {
            op = Operator.NE;
        }
        name = elem.getAttributeValue("name");
        String value = elem.getAttributeValue("value");

        return new Predicate(name, value, op);
    }

    public static Predicate fromJSON(JSONObject json) {
        String opName = json.getString("op");
        Operator op = Operator.STARTS_WITH;
        if (opName.equals(Operator.EQ.getLabel())) {
            op = Operator.EQ;
        } else if (opName.equals(Operator.NE.getLabel())) {
            op = Operator.NE;
        }
        String name = json.getString("name");
        String value = json.getString("value");
        return new Predicate(name, value, op);
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("value", value);
        json.put("op", op.getLabel());
        return json;
    }

    public static enum Operator {
        EQ("eq"), NE("ne"), STARTS_WITH("startsWith");

        private final String label;
        Operator(String label) {
            this.label = label;
        }

        public String getLabel() { return this.label; }
    }
}
