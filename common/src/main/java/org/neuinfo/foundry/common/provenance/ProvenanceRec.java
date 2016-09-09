package org.neuinfo.foundry.common.provenance;

import org.jdom2.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.openprovenance.prov.json.Converter;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.model.Attribute;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.xml.ProvFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.IOException;
import java.util.*;

/**
 * Created by bozyurt on 5/23/14.
 */
public class ProvenanceRec {
    private final Document doc;

    private ProvenanceRec(Builder builder) {
        doc = builder.doc;
    }

    public Document getDoc() {
        return doc;
    }

    public void save(String jsonFile) throws IOException {
        org.openprovenance.prov.model.ProvFactory pFactory = new ProvFactory();
        final Converter convert = new Converter(pFactory);

        convert.writeDocument(doc, jsonFile);
    }

    public String asJSON() {
        org.openprovenance.prov.model.ProvFactory pFactory = new ProvFactory();
        final Converter convert = new Converter(pFactory);
        return convert.getString(doc);
    }

    public static class Builder {
        private String nsUrl;
        private String nsPrefix;
        private org.openprovenance.prov.model.ProvFactory pFactory;
        private final Document doc;
        private Map<String, QualifiedName> idMap = new HashMap<String, QualifiedName>();
        private String lastGeneratedId;
        private Set<String> usedIdSet = new HashSet<String>();
        final DatatypeFactory factory = DatatypeFactory.newInstance();


        public Builder(String nsUrl, String nsPrefix) throws DatatypeConfigurationException {
            this.nsUrl = nsUrl;
            this.nsPrefix = nsPrefix;

            this.pFactory = new ProvFactory();
            this.doc = pFactory.newDocument();
        }

        public Builder(String nsUrl, String nsPrefix, JSONObject provJSON) throws DatatypeConfigurationException {
            this.nsUrl = nsUrl;
            this.nsPrefix = nsPrefix;

            this.pFactory = new ProvFactory();
            final Converter convert = new Converter(pFactory);
            this.doc = convert.fromString(provJSON.toString());
            final Iterator<StatementOrBundle> it = this.doc.getStatementOrBundle().iterator();
            while (it.hasNext()) {
                final StatementOrBundle sb = it.next();
                final StatementOrBundle.Kind kind = sb.getKind();
                if (kind == StatementOrBundle.Kind.PROV_ENTITY) {
                    Entity entity = (Entity) sb;
                    String localId = entity.getId().getLocalPart();
                    idMap.put(localId, entity.getId());
                    usedIdSet.add(localId);
                } else if (kind == StatementOrBundle.Kind.PROV_AGENT) {
                    Agent agent = (Agent) sb;
                    String localId = agent.getId().getLocalPart();
                    idMap.put(localId, agent.getId());
                    usedIdSet.add(localId);
                } else if (kind == StatementOrBundle.Kind.PROV_ACTIVITY) {
                    Activity activity = (Activity) sb;
                    String localId = activity.getId().getLocalPart();
                    idMap.put(localId, activity.getId());
                    usedIdSet.add(localId);
                }
            }
        }

        public String getLastGeneratedId() {
            return lastGeneratedId;
        }

        private Attribute prepAttribute(String attrStr) {
            String[] toks = attrStr.split("\\s*=\\s*");
            if (toks.length != 2) {
                throw new RuntimeException("Invalid attribute syntax Expected 'name=value'! " + attrStr);
            }
            String name = toks[0];
            String value = toks[1];
            Attribute attr = null;
            if (name.indexOf(':') != -1) {
                String[] parts = name.split(":");
                String ns = parts[0];
                if (ns.equals("prov")) {
                    String localName = parts[1];
                    attr = pFactory.newAttribute("http://www.w3.org/ns/prov#", localName, "prov", value,
                            pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd"));

                }
            } else {
                QualifiedName elemName = pFactory.newQualifiedName(this.nsUrl, name, this.nsPrefix);
                QualifiedName type = pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd");
                attr = pFactory.newAttribute(elemName, value, type);
            }
            return attr;
        }

        private String generateLocalId(StatementOrBundle.Kind kind) {
            int counter = 1;
            //UUID uuid =  UUID.randomUUID();
            String prefix = "en_";
            if (kind == StatementOrBundle.Kind.PROV_ACTIVITY) {
                prefix = "ac_";
            } else if (kind == StatementOrBundle.Kind.PROV_AGENT) {
                prefix = "ag_";
            }
            String id = prefix + counter;
            while (usedIdSet.contains(id)) {
                counter++;
                //uuid =  UUID.randomUUID();
                id = prefix + counter;
            }
            return id;
        }

        public Builder softwareAgent() {
            final String localId = generateLocalId(StatementOrBundle.Kind.PROV_AGENT);
            this.lastGeneratedId = localId;
            return softwareAgent(localId);
        }

        public Builder softwareAgentWithAttr(String... attrs) {
            final String localId = generateLocalId(StatementOrBundle.Kind.PROV_AGENT);
            this.lastGeneratedId = localId;
            return softwareAgent(localId, attrs);
        }

        public Builder softwareAgent(String localId) {
            return softwareAgent(localId);
        }

        public Builder softwareAgent(String localId, String... attributes) {
            QualifiedName id = pFactory.newQualifiedName(nsUrl, localId, nsPrefix);
            Assertion.assertTrue(!idMap.containsKey(localId));
            Assertion.assertTrue(!usedIdSet.contains(localId));
            idMap.put(localId, id);
            usedIdSet.add(localId);

            Attribute softwareAgent = pFactory.newAttribute("http://www.w3.org/ns/prov#", "type", "prov",
                    "prov:SoftwareAgent",
                    pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd"));
            List<Attribute> attrs;
            if (attributes == null) {
                attrs = Arrays.asList(softwareAgent);
            } else {
                attrs = prepAttributes(attributes);
                attrs.add(0, softwareAgent);
            }

            final Agent agent = pFactory.newAgent(id, attrs);
            doc.getStatementOrBundle().add(agent);
            return this;
        }


        public Builder entityWithAttr(String... attributes) {
            final String localId = generateLocalId(StatementOrBundle.Kind.PROV_ENTITY);
            this.lastGeneratedId = localId;
            return docEntity(localId, null, attributes);
        }

        public Builder docEntity(String localId, String label) {
            return docEntity(localId, label);
        }

        public Builder docEntity(String localId, String label, String... attributes) {
            QualifiedName id = pFactory.newQualifiedName(nsUrl, localId, nsPrefix);
            Assertion.assertTrue(!idMap.containsKey(localId));
            Assertion.assertTrue(!usedIdSet.contains(localId));
            idMap.put(localId, id);
            usedIdSet.add(localId);
            Entity entity;
            if (attributes == null) {
                entity = pFactory.newEntity(id, label);
            } else {
                List<Attribute> attrs = prepAttributes(attributes);
                entity = pFactory.newEntity(id, attrs);
            }

            doc.getStatementOrBundle().add(entity);
            return this;
        }

        private List<Attribute> prepAttributes(String[] attributes) {
            List<Attribute> attrs = new ArrayList<Attribute>(attributes.length);
            for (String attribute : attributes) {
                Attribute attr = prepAttribute(attribute);
                attrs.add(attr);
            }
            return attrs;
        }

        public Builder activityWithAttr(String type, String startDate, String endDate, String... attributes) {
            final String localId = generateLocalId(StatementOrBundle.Kind.PROV_ACTIVITY);
            this.lastGeneratedId = localId;
            return activity(localId, type, startDate, endDate, attributes);
        }

        public Builder activity(String localId, String type, String startDate, String endDate) {
            return activity(localId, type, startDate, endDate);
        }

        public Builder activity(String localId, String type, String startDate, String endDate, String... attributes) {
            QualifiedName id = pFactory.newQualifiedName(nsUrl, localId, nsPrefix);
            Assertion.assertTrue(!idMap.containsKey(localId));
            Assertion.assertTrue(!usedIdSet.contains(localId));
            idMap.put(localId, id);
            usedIdSet.add(localId);

            Attribute attr = pFactory.newAttribute("http://www.w3.org/ns/prov#", "type", "prov", type,
                    pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd"));
            final XMLGregorianCalendar startCal = startDate != null ? factory.newXMLGregorianCalendar(startDate) : null;
            final XMLGregorianCalendar endCal = endDate != null ? factory.newXMLGregorianCalendar(endDate) : null;

            if (startCal != null || endCal != null) {
                int offset = TimeZone.getDefault().getOffset(System.currentTimeMillis()) / 60000;
                // System.out.println("Time zone:" + TimeZone.getDefault().getDisplayName() + " id:" + TimeZone.getDefault().getID());

                if (startCal != null) {
                    startCal.setTimezone(offset);
                }
                if (endCal != null) {
                    endCal.setTimezone(offset);
                }
            }
            List<Attribute> attrs;
            if (attributes == null) {
                attrs = Arrays.asList(attr);
            } else {
                attrs = prepAttributes(attributes);
                attrs.add(0, attr);
            }

            final Activity activity = pFactory.newActivity(id, startCal,
                    endCal, attrs);
            doc.getStatementOrBundle().add(activity);
            return this;
        }

        public Builder wasAssociatedWith(String activityLocalId, String agentLocalId) {
            QualifiedName agentId = idMap.get(agentLocalId);
            QualifiedName activityId = idMap.get(activityLocalId);
            Assertion.assertNotNull(agentId, "agentId");
            Assertion.assertNotNull(activityId, "activityId");
            WasAssociatedWith wasAssociatedWith = pFactory.newWasAssociatedWith(null, activityId, agentId);
            doc.getStatementOrBundle().add(wasAssociatedWith);
            return this;
        }

        public Builder used(String activityLocalId, String entityLocalId) {
            QualifiedName entityId = idMap.get(entityLocalId);
            QualifiedName activityId = idMap.get(activityLocalId);
            Assertion.assertNotNull(entityId, "entityId");
            Assertion.assertNotNull(activityId, "activityId");
            final Used used = pFactory.newUsed(activityId, entityId);

            doc.getStatementOrBundle().add(used);
            return this;
        }

        public Builder wasGeneratedBy(String entityLocalId, String activityLocalId) {
            QualifiedName entityId = idMap.get(entityLocalId);
            QualifiedName activityId = idMap.get(activityLocalId);
            Assertion.assertNotNull(entityId, "entityId");
            Assertion.assertNotNull(activityId, "activityId");
            WasGeneratedBy wasGeneratedBy = pFactory.newWasGeneratedBy(null, entityId, null, activityId);
            doc.getStatementOrBundle().add(wasGeneratedBy);
            return this;
        }

        public Builder wasDerivedFrom(String generatedEntityLocalId, String usedEntityLocalId, String activityLocalId) {
            QualifiedName generatedEntityId = idMap.get(generatedEntityLocalId);
            QualifiedName usedEntityId = idMap.get(usedEntityLocalId);
            QualifiedName activityId = idMap.get(activityLocalId);
            Assertion.assertNotNull(generatedEntityId, "generatedEntityId");
            Assertion.assertNotNull(usedEntityId, "usedEntityId");
            Assertion.assertNotNull(activityId, "activityId");

            final WasDerivedFrom derived = pFactory.newWasDerivedFrom(null, generatedEntityId, usedEntityId,
                    activityId, null, null, new ArrayList<Attribute>(0));
            doc.getStatementOrBundle().add(derived);
            return this;
        }

        public ProvenanceRec build() {
            Namespace ns = Namespace.gatherNamespaces(doc);
            doc.setNamespace(ns);
            return new ProvenanceRec(this);
        }
    }
}
