package org.neuinfo.foundry.common.config;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A condition consists of ordered group of predicates grouped by <code>or</code> or <code>and</code> groups
 * Created by bozyurt on 4/24/14.
 */
public class Condition {
    private List<LogicalGroup> groups = new ArrayList<LogicalGroup>(5);


    public static Condition fromXml(Element elem) throws Exception {
        Condition cond = new Condition();
        List<Element> children = elem.getChildren();
        boolean found = false;
        int topLevelPredCount = 0;
        for (Element child : children) {
            if (child.getName().equals("or") || child.getName().equals("and")) {
                cond.groups.add(LogicalGroup.fromXml(child));
                found = true;
            } else if (child.getName().equals("predicate")) {
                topLevelPredCount++;
            } else {
                throw new Exception("Only <predicate> , <and> and <or> tags are recognized in <condition>!");
            }
        }
        if (found && topLevelPredCount > 0) {
            throw new Exception("Mixture of top level <predicate> and logical groups tags (<and> or <or>) is not allowed!");
        }
        if (!found) {
            LogicalGroup lg = new LogicalGroup(false, children.size());
            for (Element child : children) {
                lg.predicates.add(Predicate.fromXml(child));
            }
            cond.groups.add(lg);
        }
        return cond;
    }

    public static Condition fromJSON(JSONObject json) throws Exception {
        Condition cond = new Condition();
        if (json.has("logicalGroups")) {
            JSONArray logicalGroups = json.getJSONArray("logicalGroups");
            for (int i = 0; i < logicalGroups.length(); i++) {
                JSONObject lgj = logicalGroups.getJSONObject(i);
                cond.groups.add(LogicalGroup.fromJSON(lgj));
            }
        } else if (json.has("predicates")) {
            JSONArray predicates = json.getJSONArray("predicates");
            LogicalGroup lg = new LogicalGroup(false, predicates.length());
            for (int i = 0; i < predicates.length(); i++) {
                JSONObject pjo = predicates.getJSONObject(i);
                lg.predicates.add(Predicate.fromJSON(pjo));
            }
            cond.groups.add(lg);
        } else {
            throw new Exception("Condition syntax not understood!");
        }
        return cond;
    }


    public String getFirstPredicateValue() {
        LogicalGroup lg = groups.get(0);
        Predicate predicate = lg.getPredicates().get(0);
        return predicate.getValue();
    }

    public JSONObject toJSON() throws Exception {
        JSONObject json = new JSONObject();
        if (this.groups.size() == 1 && !groups.get(0).isOrGroup()) {
            LogicalGroup lg = groups.get(0);
            JSONArray jsArr = new JSONArray();
            json.put("predicates", jsArr);
            for(Predicate pred : lg.predicates) {
                jsArr.put( pred.toJSON());
            }
        } else {
            JSONArray jsArr = new JSONArray();
            json.put("logicalGroups", jsArr);
            for(LogicalGroup lg : groups) {
                jsArr.put(lg.toJSON());
            }
        }
        return json;
    }

    public boolean isSatisfied(Map<String, String> nvMap) {
        if (groups.size() == 1) {
            LogicalGroup lg = groups.get(0);
            List<Predicate> predicates = lg.getPredicates();
            boolean ok = true;
            if (!lg.isOrGroup()) {
                for (Predicate predicate : predicates) {
                    if (!nvMap.containsKey(predicate.getName())) {
                        ok = false;
                        break;
                    }
                    ok &= predicate.satisfied(nvMap.get(predicate.getName()));
                    if (!ok) {
                        break;
                    }
                }
            } else {
                ok = false;
                for (Predicate predicate : predicates) {
                    if (nvMap.containsKey(predicate.getName())) {
                        ok |= predicate.satisfied(nvMap.get(predicate.getName()));
                        if (ok) {
                            break;
                        }
                    }
                }
            }

            return ok;

        } else {
            // TODO

        }

        return false;
    }


    public static class LogicalGroup {
        boolean orGroup = false;
        List<Predicate> predicates;

        public LogicalGroup(boolean orGroup, int numPredicates) {
            this.orGroup = orGroup;
            predicates = new ArrayList<Predicate>(numPredicates);
        }

        public boolean isOrGroup() {
            return orGroup;
        }

        public List<Predicate> getPredicates() {
            return predicates;
        }

        public static LogicalGroup fromXml(Element elem) {
            boolean orGroup = elem.getName().equals("or");

            List<Element> children = elem.getChildren("predicate");
            LogicalGroup lg = new LogicalGroup(orGroup, children.size());
            for (Element child : children) {
                lg.predicates.add(Predicate.fromXml(child));
            }
            return lg;
        }

        public static LogicalGroup fromJSON(JSONObject json) {
            boolean orGroup = json.getString("lop").equals("or");
            JSONArray predicateArr = json.getJSONArray("predicates");
            LogicalGroup lg = new LogicalGroup(orGroup, predicateArr.length());
            for (int i = 0; i < predicateArr.length(); i++) {
                JSONObject js = predicateArr.getJSONObject(i);
                lg.predicates.add(Predicate.fromJSON(js));
            }
            return lg;
        }

        public JSONObject toJSON() {
            JSONObject json = new JSONObject();
            json.put("lop", orGroup ? "or" : "and");
            JSONArray jsArr = new JSONArray();
            json.put("predicates", jsArr);
            for(Predicate pred : predicates) {
                 jsArr.put( pred.toJSON() );
            }
            return json;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("LogicalGroup{");
            sb.append("orGroup=").append(orGroup);
            sb.append(", predicates=").append(predicates);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Condition{");
        sb.append("groups=").append(groups);
        sb.append('}');
        return sb.toString();
    }
}
