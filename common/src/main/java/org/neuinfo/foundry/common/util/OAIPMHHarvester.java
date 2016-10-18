package org.neuinfo.foundry.common.util;

import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.StringReader;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 11/4/14.
 */
public class OAIPMHHarvester {
    String sourceName;
    String oaiURL;
    String workDir = "/tmp";
    int sleepTimeSecs = 20; // 1
    String metaDataPrefix = "oai_dc";
    Set<String> allowedSetSpecs = new HashSet<String>(7);
    boolean sampleMode = false;

    static Logger logger = Logger.getLogger(OAIPMHHarvester.class);

    public OAIPMHHarvester(String sourceName, String oaiURL, String workDir, boolean sampleMode) {
        this.sourceName = sourceName;
        this.oaiURL = oaiURL;
        this.workDir = workDir;
        this.sampleMode = sampleMode;
    }

    public Set<String> getAllowedSetSpecs() {
        return allowedSetSpecs;
    }

    public void setAllowedSetSpecs(Set<String> allowedSetSpecs) {
        this.allowedSetSpecs = allowedSetSpecs;
    }

    public String getMetaDataPrefix() {
        return metaDataPrefix;
    }

    public void setMetaDataPrefix(String metaDataPrefix) {
        this.metaDataPrefix = metaDataPrefix;
    }

    /**
     * @param testMode
     * @return the cache directory where the data is downloaded to
     * @throws Exception
     */
    public String handle(boolean testMode) throws Exception {
        String url = oaiURL + "?verb=Identify";
        String xmlContent = getXMLContent(url);
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(new StringReader(xmlContent));
        Element rootEl = doc.getRootElement();
        // Utils.saveXML(rootEl, "/tmp/identify.xml");
        List<Namespace> nsList = rootEl.getNamespacesInScope();
        Namespace theNS = null;
        for (Namespace ns : nsList) {
            if (ns.getURI().indexOf("openarchives") != -1) {
                theNS = ns;
                break;
            }
        }
        Assertion.assertNotNull(theNS);

        Element identify = rootEl.getChild("Identify", theNS);
        String repoName = identify.getChildTextTrim("repositoryName", theNS);
        logger.info("Contacted archive:" + repoName);
        // setup archive directory

        String dirName = repoName.replaceAll("\\s+", "");
        File directory = new File(this.workDir, dirName);
        if (directory.isDirectory()) {
            // if already exists clean cache first
            Utils.deleteRecursively(directory);
        }
        directory.mkdir();
        if (!directory.isDirectory()) {
            throw new Exception("Failed to create repository folder " + directory.getAbsolutePath());
        }

        // output repository info to status file
        File infoFile = new File(directory, dirName + "_info.xml");
        Element ifRootEl = new Element("OAI-PMH");
        Element identifyClone = identify.clone().detach();

        System.out.println(toXmlString(identifyClone));
        ifRootEl.addContent(identifyClone);

        Utils.saveXML(ifRootEl, infoFile.getAbsolutePath());

        if (isOAIDCSupported(theNS)) {
            logger.info(String.format("%s archive supports %s", repoName, metaDataPrefix));
        } else {
            throw new Exception(repoName + " archive does not support oai_dc!");
        }

        if (testMode) {
            return directory.getAbsolutePath();
        }

        // loop through various sets from the target server and collect their info
        List<SetInfo> sets = new LinkedList<SetInfo>();

        url = oaiURL + "?verb=ListSets";
        xmlContent = getXMLContent(url);
        builder = new SAXBuilder();
        doc = builder.build(new StringReader(xmlContent));
        rootEl = doc.getRootElement();
        Element listSetsEl = rootEl.getChild("ListSets", theNS);
        if (listSetsEl != null) {
            List<Element> children = listSetsEl.getChildren("set", theNS);
            for (Element child : children) {
                String setSpec = child.getChildTextTrim("setSpec", theNS);
                String setName = child.getChildTextTrim("setName", theNS);
                if (!allowedSetSpecs.isEmpty()) {
                    if (allowedSetSpecs.contains(setSpec)) {
                        sets.add(new SetInfo(setSpec, setName));
                    }
                } else {
                    sets.add(new SetInfo(setSpec, setName));
                }
            }
        } else {
            // implicit set
            sets.add(new SetInfo("_IMPLICIT_", ""));
        }

        // Setup master file (i.e. file to hold records from all sets) for writing
        File masterFile = new File(directory, dirName + "_complete.xml");
        int setCounter = 1;
        int setCount = sets.size();
        BufferedWriter masterOut = null;
        try {
            masterOut = Utils.newUTF8CharSetWriter(masterFile.getAbsolutePath());
            masterOut.write("<?xml version=\"1.0\"?>");
            masterOut.newLine();
            masterOut.write("<ListRecords>");
            masterOut.newLine();
            for (SetInfo si : sets) {
                logger.info(String.format("Begin Processing Set [%d out of %d]: %s (%s) %n%n", setCounter,
                        setCount, si.setName, si.setSpec));


                //Setup record counters to verify ingestion as it progresses
                int totalRecords = 0;
                int currentRecords = 0;
                // Construct base URL for fetching records
                String baseURL = oaiURL + "?verb=ListRecords";

                //Setup appropriate parameters for the target server and current record set
                String initialParams = "&metadataPrefix=" + metaDataPrefix;
                if (!si.setSpec.equals("_IMPLICIT_")) {
                    initialParams += "&set=" + si.setSpec;
                }

                //Setup appropriate parameters for the target server in case a resumption token is provided
                String resumptionBase = "&resumptionToken=";
                String resumptionToken = "initial";

                //Setup record set specific data file
                File xmlFile = new File(directory, dirName + "_" + si.setSpec + "_" + prepDate() + ".xml"); // FIXME add date

                //Setup counter to track number of requests to download complete record set
                int fetchCounter = 1;
                BufferedWriter xmlOut = null;
                try {
                    xmlOut = Utils.newUTF8CharSetWriter(xmlFile.getAbsolutePath());

                    xmlOut.write("<?xml version=\"1.0\"?>");
                    xmlOut.newLine();

                    url = null;
                    while (!resumptionToken.equals("")) {
                        if (fetchCounter == 1) {
                            //First call to fetch records will never have a resumption token as a parameter
                            url = baseURL + initialParams;
                            //Clear resumption token on first pass
                            resumptionToken = "";
                        } else {
                            url = baseURL + resumptionBase + URLEncoder.encode(resumptionToken, "UTF-8");
                        }

                        //Now fetch records from OAI-PMH server
                        logger.info("URL " + fetchCounter + " being processed: " + url);


                        xmlContent = getXMLContent(url, 5);
                        if (xmlContent == null) {
                            Utils.close(masterOut);
                            masterOut = null;
                            masterFile.delete();
                            throw new Exception("Cannot get XML from " + url + " after 5 trials!");
                        }

                        //Run through the result and write records to local file validating record count
                        builder = new SAXBuilder();
                        doc = builder.build(new StringReader(xmlContent));
                        rootEl = doc.getRootElement();
                        Element lrEl = rootEl.getChild("ListRecords", theNS);
                        if (lrEl != null) {
                            List<Namespace> namespacesInScope = lrEl.getNamespacesInScope();
                            Element lrRootEl = new Element("ListRecords", theNS);
                            for (Namespace ns : namespacesInScope) {
                                if (!ns.equals(theNS)) {
                                    lrRootEl.addNamespaceDeclaration(ns);
                                }
                            }
                            xmlOut.write(toXmlString(lrRootEl).replaceFirst("/>$", ">"));
                            xmlOut.newLine();
                        } else {
                            xmlOut.write("<ListRecords>");
                            xmlOut.newLine();
                        }

                        Element resumptionTokenEl = null;
                        int recordValidator = 0;
                        if (lrEl != null) {
                            resumptionTokenEl = lrEl.getChild("resumptionToken", theNS);
                            List<Element> recordEls = lrEl.getChildren("record", theNS);
                            currentRecords = recordEls.size();

                            for (Element recordEl : recordEls) {
                                //Add repository, setName, URL to header of OAI-PMH XML so
                                // that output XML has all information about request and results
                                Element reClone = recordEl.clone();
                                Element header = reClone.getChild("header", theNS);
                                Assertion.assertNotNull(header);
                                header.addContent(new Element("repository").setText(repoName));
                                header.addContent(new Element("setName").setText(si.setName));
                                header.addContent(new Element("fetchURL").setText(url));
                                header.addContent(new Element("recNum").setText(String.valueOf(recordValidator)));

                                String xmlString = toXmlString(reClone);
                                xmlOut.write(xmlString);
                                xmlOut.newLine();
                                masterOut.write(xmlString);
                                masterOut.newLine();

                                recordValidator++;
                            }
                        }
                        if (resumptionTokenEl == null) {
                            totalRecords += currentRecords;
                            logger.info(String.format("Records added: %d     Total Records: %d",
                                    currentRecords, totalRecords));
                            logger.info(String.format("All records fetched (%d) - no resumption token", totalRecords));
                            resumptionToken = "";
                        } else {
                            resumptionToken = resumptionTokenEl.getTextTrim();
                            logger.info("Resumption Token: " + resumptionToken);
                            // currentRecords--;
                            totalRecords += currentRecords;
                            logger.info(String.format("Records added: %d   Total Records: %d",
                                    currentRecords, totalRecords));
                            if (currentRecords == 0) {
                                // If there is an error in fetching records then exit
                                throw new Exception("No data loaded from last URL. Exiting...");
                            }
                        }
                        if (recordValidator != currentRecords) {
                            Utils.close(masterOut);
                            masterOut = null;
                            masterFile.delete();
                            logger.warn(String.format("recordValidator:%d currentRecords:%d", recordValidator, currentRecords));
                            throw new Exception("Number of elements imported does not match. Exiting...");
                        }
                        // Increment record request for complete record set
                        fetchCounter++;
                        if (sampleMode) {
                            break;
                        }

                        //Play nice with OAI-PMH servers
                        try {
                            Thread.sleep(sleepTimeSecs * 1000L);
                        } catch (InterruptedException ie) {
                            // no op
                        }

                    } // Work on next record request within record set
                    xmlOut.write("</ListRecords>");
                    xmlOut.newLine();
                    setCounter++;
                } finally {
                    Utils.close(xmlOut);
                }
                if (sampleMode) {
                    break;
                }
            }// for si
            masterOut.write("</ListRecords>");
            masterOut.newLine();
        } finally {
            Utils.close(masterOut);
        }
        return directory.getAbsolutePath();
    }

    public static String prepDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
        return sdf.format(new Date());
    }

    public static String toXmlString(Element el) {
        XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
        String s = xout.outputString(el);
        // s = s.replaceAll("xmlns\\s*=\\s*\"[^\"]*\"", "");
        // s = s.replaceFirst("\\s+>",">");
        return s;
    }

    public static class SetInfo {
        String setSpec;
        String setName;

        public SetInfo(String setSpec, String setName) {
            this.setSpec = setSpec;
            this.setName = setName;
        }
    }

    boolean isOAIDCSupported(Namespace theNS) throws Exception {
        String url = this.oaiURL + "?verb=ListMetadataFormats";
        String xmlContent = getXMLContent(url);
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(new StringReader(xmlContent));
        Element rootEl = doc.getRootElement();
        List<Element> children = rootEl.getChild("ListMetadataFormats", theNS).getChildren("metadataFormat", theNS);
        for (Element el : children) {
            String prefix = el.getChildTextTrim("metadataPrefix", theNS);
            if (prefix.equals(metaDataPrefix)) {
                return true;
            }
        }
        return false;
    }

    String getXMLContent(String ingestURL, int numTries) {
        System.out.println("getXMLContent: " + ingestURL);
        int urlTry = 1;
        int errorWait;
        while (urlTry > 0) {
            try {
                String xmlContent = getXMLContent(ingestURL);
                return xmlContent;
            } catch (Throwable t) {
                if (urlTry < numTries) {
                    errorWait = this.sleepTimeSecs * urlTry;
                    logger.warn(String.format("Load (%d) of XML from URL failed. Retrying in %d seconds",
                            urlTry, errorWait));
                    logger.warn("XML", t);
                    try {
                        Thread.sleep(errorWait * 1000L);
                    } catch (InterruptedException e) {
                        // no op
                    }
                    urlTry++;
                } else {
                    logger.error(String.format("Load of XML from URL failed %d times. Exiting...", urlTry));
                    logger.error("XML", t);
                }
            }
        }
        return null;
    }

    public static String getXMLContent(String ingestURL) throws Exception {
        return Utils.sendGetRequest(ingestURL);
    }


    public static void main(String[] args) throws Exception {
        String oaiURL = "http://www.datadryad.org/oai/request";
        String sourceName = "Dryad";
        File workDir = new File("/var/burak/tmp");

        OAIPMHHarvester harvester = new OAIPMHHarvester(sourceName, oaiURL, workDir.getAbsolutePath(), false);

        harvester.handle(false);

    }
}
