package org.neuinfo.foundry.common.transform;

import org.apache.commons.cli.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.JsonNode;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by bozyurt on 7/30/15.
 */
public class IdentityTransformationGenerator {

    public static JSONObject combineJSONObjects(File jsonDir) throws IOException {
        File[] files = jsonDir.listFiles(new Utils.RegexFileNameFilter("\\.json$"));
        JSONObject mainJSON = new JSONObject();
        boolean first = true;
        for (File f : files) {
            String s = Utils.loadAsString(f.getAbsolutePath());
            JSONObject js = new JSONObject(s);
            if (first) {
                mainJSON = js;
                first = false;
            } else {
                for (String key : js.keySet()) {
                    mainJSON.put(key, js.get(key));
                }
            }
        }
        return mainJSON;

    }

    List<String> generateRules(JSONObject exampleOrigData) {
        JsonNode root = JSONUtils.toTraversableJSONDOM(exampleOrigData);
        List<JsonNode> leafNodes = JSONUtils.findAllLeafNodes(root);
        Set<String> seenSet = new HashSet<String>();
        List<String> rules = new ArrayList<String>(leafNodes.size());
        for (JsonNode leafNode : leafNodes) {
            String transformRule = prepareTransformRule(leafNode);
            if (!transformRule.isEmpty() && !seenSet.contains(transformRule)) {
                seenSet.add(transformRule);
                rules.add(transformRule);
            }
        }
        return rules;
    }

    public String generateTransformScript(JSONObject exampleOrigData) {
        StringBuilder sb = new StringBuilder(4096);
        List<String> rules = generateRules(exampleOrigData);
        for (String rule : rules) {
            sb.append(rule).append('\n');
        }
        List<String> correctedRules = postprocess(rules, sb.toString());
        sb.setLength(0);
        for (String rule : correctedRules) {
            sb.append(rule).append('\n');
        }

        return sb.toString();
    }

    List<String> postprocess(List<String> rules, String trScript) {
        TransformationFunctionRegistry registry = TransformationFunctionRegistry.getInstance();
        TransformationLanguageInterpreter interpreter = new TransformationLanguageInterpreter(registry);
        interpreter.parse(trScript);
        List<String> correctedRules = new ArrayList<String>();
        List<Transformation> transformations = interpreter.getTransformations();
        Map<String, List<TRInfo>> map = new HashMap<String, List<TRInfo>>();
        Map<String, List<TRInfo>> badMap = new HashMap<String, List<TRInfo>>();
        Map<String, TRInfo> rule2TRIMap = new HashMap<String, TRInfo>();
        for (int i = 0; i < transformations.size(); i++) {
            Transformation tr = transformations.get(i);
            String rule = rules.get(i);
            TRInfo trInfo = new TRInfo(tr, rule);
            rule2TRIMap.put(rule, trInfo);
            String key = trInfo.getSecond2LastTokenOfToPart();
            if (key != null) {
                List<TRInfo> trInfos = map.get(key);
                if (trInfos == null) {
                    trInfos = new LinkedList<TRInfo>();
                    map.put(key, trInfos);
                }
                trInfos.add(trInfo);
            }
            // if (trInfo.doesFromPartHasArray()) {
            String fromKey = trInfo.getFromKey();
            List<TRInfo> triList = badMap.get(fromKey);
            if (triList == null) {
                triList = new LinkedList<TRInfo>();
                badMap.put(fromKey, triList);
            }
            triList.add(trInfo);
            // }
        }

        Set<String> badRuleSet = new HashSet<String>();
        for (String fromKey : badMap.keySet()) {
            List<TRInfo> triList = badMap.get(fromKey);
            if (triList.size() > 1) {
                TRInfo maxTR = null;
                int max = Integer.MIN_VALUE;
                for (TRInfo tri : triList) {
                    int numArrs = countArrays(tri.getFromPart());
                    if (max < numArrs) {
                        maxTR = tri;
                        max = numArrs;
                    }
                }
                for (TRInfo tri : triList) {
                    if (tri != maxTR) {
                        int numArrs = countArrays(tri.getFromPart());
                        if (numArrs < max) {
                            badRuleSet.add(tri.rule);
                        }
                    }
                }
            }
        }

        for (int i = 0; i < transformations.size(); i++) {
            Transformation tr = transformations.get(i);
            String rule = rules.get(i);
            if (badRuleSet.contains(rule)) {
                continue;
            }
            String toPart = tr.getDestColumnName();
            int idx = toPart.lastIndexOf('.');
            if (idx == -1) {
                correctedRules.add(rule);
            } else {
                String suffix = toPart.substring(idx + 1);
                List<TRInfo> trInfos = map.get(suffix);
                if (trInfos == null) {
                    correctedRules.add(rule);
                } else {
                    if (hasMatchingRules(toPart.substring(0, idx), trInfos)) {
                        String cr = correctRule(tr);
                        rule2TRIMap.put(cr, new TRInfo(tr, cr));
                        correctedRules.add(cr);
                    }
                }
            }
        }
        List<TRInfo> correctedTRIList = new ArrayList<TRInfo>(correctedRules.size());
        for (String cr : correctedRules) {
            TRInfo trInfo = rule2TRIMap.get(cr);
            Assertion.assertNotNull(trInfo);
            correctedTRIList.add(trInfo);
        }
        Collections.sort(correctedTRIList, new Comparator<TRInfo>() {
            @Override
            public int compare(TRInfo o1, TRInfo o2) {
                return o1.tr.getSourceColumnNames().get(0).compareTo(o2.tr.getSourceColumnNames().get(0));
            }
        });
        correctedRules.clear();
        for (TRInfo trInfo : correctedTRIList) {
            correctedRules.add(trInfo.rule);
        }
        return correctedRules;
    }

    static int countArrays(String fromPart) {
        int count = 0;
        int offset = 0;
        while (offset < fromPart.length()) {
            int idx = fromPart.indexOf('[', offset);
            if (idx == -1) {
                break;
            }
            count++;
            offset = idx + 1;
        }
        return count;
    }

    static String correctRule(Transformation tr) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("transform ");
        if (tr.getSourceColumnNames().size() > 1) {
            sb.append("columns ");
            for (Iterator<String> iter = tr.getSourceColumnNames().iterator(); iter.hasNext(); ) {
                String scn = iter.next();
                sb.append('"').append(scn).append('"');
                if (iter.hasNext()) {
                    sb.append(" , ");
                }
            }
        } else {
            sb.append("column \"").append(tr.getSourceColumnNames().get(0)).append('"');
        }
        sb.append(" to \"");
        sb.append(tr.getDestColumnName()).append("._$\";");

        return sb.toString();
    }

    static boolean hasMatchingRules(String refToPartPrefix, List<TRInfo> trInfos) {
        for (TRInfo trInfo : trInfos) {
            if (trInfo.getToPart().startsWith(refToPartPrefix)) {
                return true;
            }
        }
        return false;
    }

    static class TRInfo {
        Transformation tr;
        String rule;

        public TRInfo(Transformation tr, String rule) {
            this.tr = tr;
            this.rule = rule;
        }

        String getToPart() {
            return tr.getDestColumnName();
        }

        String getSecond2LastTokenOfToPart() {
            String destColumnName = tr.getDestColumnName();
            String[] tokens = destColumnName.split("\\.");
            if (tokens.length < 2) {
                return null;
            }
            return tokens[tokens.length - 2];
        }

        boolean doesFromPartHasArray() {
            String fromPart = tr.getSourceColumnNames().get(0);
            return fromPart.indexOf("[*]") != -1;
        }

        String getFromPart() {
            return tr.getSourceColumnNames().get(0);
        }

        String getFromKey() {
            String fromPart = tr.getSourceColumnNames().get(0);
            fromPart = fromPart.replaceAll("\\[\\*?\\]", "");
            return fromPart;
        }

    }


    String prepareTransformRule(JsonNode leaf) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("transform column ");
        List<JsonNode> path = new ArrayList<JsonNode>(10);
        JsonNode n = leaf;
        while (n != null) {
            path.add(n);
            n = n.getParent();
            if (n != null && n.getParent() == null) {
                break;
            }
        }

        Collections.reverse(path);
        sb.append("\"$.");
        for (Iterator<JsonNode> it = path.iterator(); it.hasNext(); ) {
            JsonNode node = it.next();
            if (node.isArray()) {
                sb.append("'").append(node.getName()).append("'[*]");
            } else {
                sb.append("'").append(node.getName()).append("'");
            }
            if (it.hasNext()) {
                sb.append('.');
            }
        }

        sb.append('"').append(" to \"");
        for (Iterator<JsonNode> it = path.iterator(); it.hasNext(); ) {
            JsonNode node = it.next();
            if (!node.getName().equals("_$")) {
                if (node.isArray()) {
                    sb.append(node.getName()).append("[]");
                } else {
                    sb.append(node.getName());
                }
                if (it.hasNext()) {
                    sb.append('.');
                }
            }
        }
        if (sb.charAt(sb.length() - 1) == '.') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("\";");

        return sb.toString();
    }


    static void combineAndGenerate(String jsonDir) throws IOException {
        // /tmp/lincs_ds_result
        // JSONObject json = IdentityTransformationGenerator.combineJSONObjects(new File(jsonDir));
        File[] files = new File(jsonDir).listFiles(new Utils.RegexFileNameFilter("\\.json$"));
        // System.out.println(json.toString(2));
        IdentityTransformationGenerator itg = new IdentityTransformationGenerator();
        LinkedHashSet<String> uniqRulesSet = new LinkedHashSet<String>();
        for (File f : files) {
            String jsonStr = Utils.loadAsString(f.getAbsolutePath());
            JSONObject json = new JSONObject(jsonStr);
            List<String> rules = itg.generateRules(json);
            for (String rule : rules) {
                uniqRulesSet.add(rule);
            }
        }
        List<String> combinedRules = new ArrayList<String>(uniqRulesSet);
        StringBuilder sb = new StringBuilder(4096);
        for (String rule : combinedRules) {
            sb.append(rule).append('\n');
        }
        List<String> correctedRules = itg.postprocess(combinedRules, sb.toString());
        sb.setLength(0);
        for (String rule : correctedRules) {
            sb.append(rule).append('\n');
        }
        System.out.println(sb.toString());
        // System.out.println(itg.generateTransformScript(json));
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("IdentityTransformationGenerator", options);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        Option help = new Option("h", "print this message");
        Option sourceOption = Option.builder("s").argName("source-data-json-file|data-json-dir")
                .hasArg().required().build();
        Option combineOption = Option.builder("c").
                desc("if set combine all json files to generate the transformation script")
                .build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(sourceOption);
        options.addOption(combineOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line.hasOption("h")) {
            usage(options);
        }
        String sourcePath = line.getOptionValue('s');

        boolean combine = line.hasOption('c');

        String jsonStr; // Utils.loadAsString("/tmp/lincs_cells_csv_record.json");
        //jsonStr = Utils.loadAsString("/tmp/lincs_sm_csv_record.json");
        //jsonStr = Utils.loadAsString("/tmp/geo_platforms_xml_record.json");
        //jsonStr = Utils.loadAsString("/tmp/lincs_ds_summary_csv_record.json");
        //jsonStr = Utils.loadAsString("/tmp/medline_sample_1.json");
        // jsonStr = Utils.loadAsString("/tmp/array_express_sample.json");
        //jsonStr = Utils.loadAsString("/tmp/gemma_csv_record_1.json");
        //combineAndGenerate("/tmp/bioproject");

        if (!combine) {
            if (!new File(sourcePath).isFile()) {
                System.err.println("A valid source data json file path is expected!");
                usage(options);
            }
            jsonStr = Utils.loadAsString(sourcePath);

            JSONObject docJSON = new JSONObject(jsonStr);
            IdentityTransformationGenerator itg = new IdentityTransformationGenerator();
            System.out.println(itg.generateTransformScript(docJSON));
        } else {
            if (!new File(sourcePath).isDirectory()) {
                System.err.println("A valid source data json directory path is expected!");
                usage(options);
            }
            combineAndGenerate(sourcePath);
        }


    }
}
