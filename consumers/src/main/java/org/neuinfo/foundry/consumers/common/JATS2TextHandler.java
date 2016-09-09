package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.util.Utils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.*;

/**
 * Created by bozyurt on 4/18/16.
 */
public class JATS2TextHandler extends DefaultHandler {
    private StringBuilder textBuf = new StringBuilder(4096);
    private List<String> urls = new ArrayList<String>();
    boolean inArticleID = false;
    boolean inParagraph = false;
    boolean inTitleGroup = false;
    private String articleType;
    private StringBuilder tagContentBuf = new StringBuilder(128);
    private StringBuilder paraBuf = new StringBuilder(256);
    private int urlStartIdx = -1;
    private Set<String> paraUrlSet = new HashSet<String>(7);
    private boolean inAuthorNotes = false;
    private boolean inPermissions = false;
    private boolean inRef = false;
    private boolean inTable = false;
    private boolean inTD = false;
    /**
     * true if some text is added inside a tag
     */
    private boolean textAdded = false;
    private ArticleInfo ai = null;
    private SectionInfo curSi;
    boolean inSec = false, inTitle = false, inJournalTitle = false;
    boolean inArticleTitle = false;
    private StringBuilder buf = new StringBuilder(256);
    private OpType opType = OpType.URL;
    private boolean inEligibleSec = false;
    Stack<String> sectionStack = new Stack<String>();
    private Set<String> tagSet = new HashSet<String>();
    boolean inSup = false;
    /**
     * indicates references to bibliography
     */
    boolean inXref = false;

    public static enum OpType {
        URL, NER
    }

    public JATS2TextHandler(String filePath, OpType opType) {
        ai = new ArticleInfo(filePath);
        this.opType = opType;
        String[] tags = {"ext-link", "article-id", "p", "author-notes", "permissions", "title",
                "journal-title", "article-title", "sec", "sec-type", "title-group", "comment"};
        Collections.addAll(tagSet, tags);
    }

    public JATS2TextHandler(String filePath) {
        this(filePath, OpType.URL);
    }

    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        if (inArticleID) {
            tagContentBuf.append(ch, start, length);
        }
        if (inParagraph) {
            paraBuf.append(ch, start, length).append(' ');
        }
        if (!inAuthorNotes && !inPermissions) {
            if (opType != OpType.NER) {
                textBuf.append(ch, start, length).append(' ');
            } else {
                if (inEligibleSec) {
                    if (inTable || inTD) {
                        int idx = textBuf.length() - 2;
                        if (idx >= 0 && textBuf.charAt(idx) != '.' && textBuf.charAt(idx + 1) != '.') {
                            textBuf.append(". ");
                        }
                        textBuf.append(ch, start, length).append(' ');
                    } else {
                        if (!inXref) {
                            textBuf.append(ch, start, length);
                            textAdded = true;
                        }
                    }
                }
            }
        }
        if (inTitle || inJournalTitle || inArticleTitle) {
            buf.append(ch, start, length);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName,
                             Attributes attributes) throws SAXException {
        //System.out.println(">> " + localName);
        localName = localName.toLowerCase();
        textAdded = false;

        if (localName.equals("ext-link")) {
            String url = attributes.getValue("xlink:href");
            if (url != null) {
                if (!inAuthorNotes && !inPermissions & !inRef) {
                    urls.add(url);
                    paraUrlSet.add(url);
                }
            }
        } else if (localName.equals("article-id")) {
            String type = attributes.getValue("pub-id-type");
            if (type != null) {
                articleType = type;
            }
            inArticleID = true;
        } else if (localName.equals("title-group")) {
            inTitleGroup = true;
        } else if (localName.equals("p")) {
            urlStartIdx = urls.size() - 1;
            inParagraph = true;
        } else if (localName.equals("author-notes")) {
            inAuthorNotes = true;
        } else if (localName.equals("permissions")) {
            inPermissions = true;
        } else if (localName.equals("title")) {
            inTitle = true;
        } else if (localName.equals("journal-title")) {
            inJournalTitle = true;
        } else if (localName.equals("article-title")) {
            inArticleTitle = true;
        } else if (localName.equals("sec")) {
            inSec = true;
            curSi = new SectionInfo();
            String type = attributes.getValue("sec-type");
            if (type != null) {
                curSi.type = type;
            }
            if (this.inEligibleSec) {
                sectionStack.push("sec");
            }
        } else if (localName.equals("ref")) {
            inRef = true;
        } else if (localName.equals("table")) {
            inTable = true;
        } else if (localName.equals("td")) {
            inTD = true;
        } else if (localName.equals("sup")) {
            inSup = true;
        } else if (localName.equals("xref")) {
            inXref = true;
        }
    }

    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        localName = localName.toLowerCase();

        if (localName.equals("article-id")) {
            if (opType != OpType.NER) {
                addEOS();
            }
            inArticleID = false;
            if (articleType.equals("pmid")) {
                ai.PMID = tagContentBuf.toString().trim();
            } else if (articleType.equals("pmc")) {
                ai.PMCID = tagContentBuf.toString().trim();
            }
        } else if (localName.equals("p")) {
            if (opType != OpType.NER) {
                addEOS();
            }
            inParagraph = false;
            String para = paraBuf.toString().trim();
            ai.paraList.add(para);
            if (!urls.isEmpty() && !paraUrlSet.isEmpty()) {
                int startOffset = Math.max(0, urlStartIdx);
                for (int i = startOffset; i < urls.size(); i++) {
                    String url = urls.get(i);
                    // uliList.add(new URLLocInfo(url, para));
                }
            }
            paraBuf.setLength(0);
            paraUrlSet.clear();
        } else if (localName.equals("ext-link") && inTable) {
            String para = paraBuf.toString().trim();
            if (!Utils.isEmpty(para)) {
                ai.paraList.add(para);
            }
            if (!urls.isEmpty() && !paraUrlSet.isEmpty()) {
                int startOffset = Math.max(0, urlStartIdx);
                for (int i = startOffset; i < urls.size(); i++) {
                    String url = urls.get(i);
                   // uliList.add(new URLLocInfo(url, !Utils.isEmpty(para) ? para : url));
                }
            }
            paraBuf.setLength(0);
            paraUrlSet.clear();

        } else if (localName.equals("author-notes")) {
            inAuthorNotes = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("permissions")) {
            inPermissions = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("sec")) {
            if (opType != OpType.NER) {
                addEOS();
            }
            inSec = false;
            if (inEligibleSec) {
                sectionStack.pop();
                if (sectionStack.empty()) {
                    this.inEligibleSec = false;
                }
            }
            if (curSi != null) {
                ai.siList.add(curSi);
            }
            curSi = null;
        } else if (localName.equals("title")) {
            if (inSec) {
                if (curSi != null) {
                    curSi.title = buf.toString().trim();
                    ai.siList.add(curSi);
                    if (opType == OpType.NER && isSectionEligible(curSi)) {
                        this.inEligibleSec = true;
                        sectionStack.push("sec");
                    }
                    curSi = null;
                }
            }
            if (opType != OpType.NER) {
                addEOS();
            }
            inTitle = false;
        } else if (localName.equals("journal-title")) {
            ai.journalTitle = buf.toString().trim();
            inJournalTitle = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("article-title")) {
            if (inTitleGroup) {
                ai.title = buf.toString().trim();
            }
            inArticleTitle = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("title-group")) {
            inTitleGroup = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("ref")) {
            inRef = false;
        } else if (localName.equals("table")) {
            inTable = false;
        } else if (localName.equals("td")) {
            inTD = false;
            if (opType != OpType.NER) {
                addEOS();
            }
        } else if (localName.equals("sup")) {
            inSup = false;
        } else if (localName.equals("xref")) {
            inXref = false;
        }

        buf.setLength(0);
        tagContentBuf.setLength(0);
        if (textAdded && textBuf.length() > 0) {
            textBuf.append(' ');
        }
        textAdded = false;
    }

    void addEOS() {
        int idx = textBuf.length() - 1;
        boolean foundPeriod = false;
        while (idx >= 0) {
            char c = textBuf.charAt(idx);
            if (!Character.isWhitespace(c) && c != '.') {
                break;
            }
            if (c == '.') {
                foundPeriod = true;
                break;
            }
            idx--;
        }
        if (!foundPeriod) {
            char c = textBuf.charAt(textBuf.length() - 1);
            if (!Character.isWhitespace(c)) {
                textBuf.append(' ');
            }
            textBuf.append(". \n");
        }
    }

    public String getText() {
        return textBuf.toString();
    }

    public List<String> getUrls() {
        return urls;
    }

    public String getPMID() {
        return ai.getPMID();
    }

    public String getPMCID() {
        return ai.getPMCID();
    }


    public List<String> getParaList() {
        return ai.getParaList();
    }

    public ArticleInfo getArticleInfo() {
        return ai;
    }

    private boolean isSectionEligible(SectionInfo si) {
        String sectionTitle = si.getTitle().toLowerCase();
        return sectionTitle.equals("techniques")
                || sectionTitle.contains("methods")
                || sectionTitle.contains("procedures")
                || sectionTitle.contains("method")
                || sectionTitle.contains("procedure")
                || sectionTitle.contains("experiment");
    }

    public static class ArticleInfo {
        String filePath;
        String journalTitle;
        String title;
        private String PMID;
        private String PMCID;
        List<SectionInfo> siList = new ArrayList<SectionInfo>(10);
        List<String> paraList = new ArrayList<String>();

        public ArticleInfo(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ArticleInfo [");
            if (filePath != null) {
                builder.append("filePath=");
                builder.append(filePath);
                builder.append(", ");
            }
            if (journalTitle != null) {
                builder.append("journalTitle=");
                builder.append(journalTitle);
                builder.append(", ");
            }
            if (title != null) {
                builder.append("title=");
                builder.append(title);
                builder.append(", ");
            }
            builder.append("PMID=").append(PMID).append(",");
            if (siList != null) {
                for (SectionInfo si : siList) {
                    builder.append("\n\t").append(si);
                }
            }
            builder.append("]");
            return builder.toString();
        }

        public String getFilePath() {
            return filePath;
        }

        public String getJournalTitle() {
            return journalTitle;
        }

        public String getTitle() {
            return title;
        }

        public String getPMID() {
            return PMID;
        }

        public String getPMCID() {
            return PMCID;
        }

        public List<SectionInfo> getSiList() {
            return siList;
        }

        public List<String> getParaList() {
            return paraList;
        }

    }// ;

    public static class SectionInfo {
        String type;
        String title;

        public SectionInfo() {
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("SectionInfo [");
            if (title != null) {
                builder.append("title=");
                builder.append(title);
                builder.append(", ");
            }
            if (type != null) {
                builder.append("type=");
                builder.append(type);
            }

            builder.append("]");
            return builder.toString();
        }

        public String getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }
    }// ;
}
