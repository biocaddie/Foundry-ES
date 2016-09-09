package org.neuinfo.foundry.common.util;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;

import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by bozyurt on 10/27/14.
 */
public class RSSFeedParser implements IDataParser{
    private RSSFeed feed;

    public RSSFeedParser() {
    }

    public static void prepPubDate(Element el, SimpleDateFormat sdf, RSSFeed rf) {
        if (el.getChild("pubDate") != null) {
            rf.pubDate = prepPubDate(el, sdf);
        }
    }

    public static Date prepPubDate(Element el, SimpleDateFormat sdf) {
        try {
            return sdf.parse(el.getChildTextTrim("pubDate"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public RSSFeed getFeed() {
        return feed;
    }

    @Override
    public void initialize(InputDataIterator iterator) throws Exception {
        URL rssURL = iterator.getUrl();
        SAXBuilder builder = new SAXBuilder();

        Document doc = builder.build(rssURL);
        Element rootEl = doc.getRootElement();
        Element channelEl = rootEl.getChild("channel");

        this.feed = RSSFeed.fromXML(channelEl);
    }

    @Override
    public JSONObject toJSON() throws Exception {
        return null;
    }

    public static class RSSFeed {
        String description = "";
        String title = "";
        String link = "";
        String language = "";
        String copyright = "";
        Date pubDate;
        List<RSSItem> items = new ArrayList<RSSItem>();

        public RSSFeed(String title, String link) {
            this.title = title;
            this.link = link;
        }

        public String getDescription() {
            return description;
        }

        public String getTitle() {
            return title;
        }

        public String getLink() {
            return link;
        }

        public String getLanguage() {
            return language;
        }

        public String getCopyright() {
            return copyright;
        }


        public Date getPubDate() {
            return pubDate;
        }

        public List<RSSItem> getItems() {
            return this.items;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RSSFeed{");
            sb.append("description='").append(description).append('\'');
            sb.append(", title='").append(title).append('\'');
            sb.append(", link='").append(link).append('\'');
            sb.append(", language='").append(language).append('\'');
            sb.append(", copyright='").append(copyright).append('\'');
            sb.append(", pubDate=").append(pubDate);
            if (!items.isEmpty()) {
                sb.append("\nitems=");
                for (RSSItem item : items) {
                    sb.append("\t").append(item).append("\n");

                }
            }
            sb.append('}');
            return sb.toString();
        }

        public static RSSFeed fromXML(Element el) {
            SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
            String title = el.getChildTextTrim("title");
            String link = el.getChildTextTrim("link");
            String description = el.getChildTextTrim("description");
            RSSFeed rf = new RSSFeed(title, link);
            rf.description = description;
            prepPubDate(el, sdf, rf);
            if (el.getChild("language") != null) {
                rf.language = el.getChildTextTrim("language");
            }
            if (el.getChild("copyright") != null) {
                rf.copyright = el.getChildTextTrim("copyright");
            }

            List<Element> itemEls = el.getChildren("item");
            for (Element ie : itemEls) {
                rf.items.add(RSSItem.fromXML(ie));
            }
            return rf;
        }
    }

    public static class RSSItem {
        String title = "";
        String link = "";
        String description = "";
        String author = "";
        String guid = "";
        Date pubDate;
        String source = "";
        String comments;
        List<RSSCategory> categories = new ArrayList(3);


        public String getTitle() {
            return title;
        }

        public String getLink() {
            return link;
        }

        public String getDescription() {
            return description;
        }

        public String getAuthor() {
            return author;
        }

        public String getGuid() {
            return guid;
        }

        public Date getPubDate() {
            return pubDate;
        }

        public String getSource() {
            return source;
        }

        public String getComments() {
            return comments;
        }

        public List<RSSCategory> getCategories() {
            return categories;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RSSItem{");
            sb.append("title='").append(title).append('\'');
            sb.append(", link='").append(link).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append(", author='").append(author).append('\'');
            sb.append(", guid='").append(guid).append('\'');
            sb.append(", pubDate=").append(pubDate);
            sb.append(", source='").append(source).append('\'');
            sb.append(", comments='").append(comments).append('\'');
            sb.append(", categories=").append(categories);
            sb.append('}');
            return sb.toString();
        }

        public static RSSItem fromXML(Element el) {
            SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
            RSSItem ri = new RSSItem();
            ri.title = el.getChildTextTrim("title");
            if (el.getChild("link") != null) {
                ri.link = el.getChildTextTrim("link");
            }
            if (el.getChild("description") != null) {
                ri.description = el.getChildTextTrim("description");
            }
            if (el.getChild("author") != null) {
                ri.author = el.getChildTextTrim("author");
            }
            if (el.getChild("guid") != null) {
                ri.guid = el.getChildTextTrim("guid");
            }
            if (el.getChild("source") != null) {
                ri.source = el.getChildTextTrim("source");
            }
            if (el.getChild("comments") != null) {
                ri.comments = el.getChildTextTrim("comments");
            }
            if (el.getChild("pubDate") != null) {
                ri.pubDate = prepPubDate(el, sdf);
            }

            List<Element> catEls = el.getChildren("category");
            for (Element ce : catEls) {
                ri.categories.add(RSSCategory.fromXML(ce));
            }

            return ri;
        }

    }

    public static class RSSCategory {
        final String category;
        final String domain;

        public RSSCategory(String category, String domain) {
            this.category = category;
            this.domain = domain;
        }

        public String getCategory() {
            return category;
        }

        public String getDomain() {
            return domain;
        }

        public static RSSCategory fromXML(Element el) {
            String domain = "";
            if (el.getAttribute("domain") != null) {
                domain = el.getAttributeValue("domain");
            }
            return new RSSCategory(el.getTextTrim(), domain);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RSSCategory{");
            sb.append("category='").append(category).append('\'');
            sb.append(", domain='").append(domain).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * http://feeds.wired.com/wiredscience
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String rssUrl = "http://feeds.wired.com/wiredscience";
        InputDataIterator iter = new InputDataIterator(new URL(rssUrl));
        RSSFeedParser parser = (RSSFeedParser) DataParserFactory.getInstance().createDataParser("rss", iter);

        RSSFeed feed = parser.getFeed();

        System.out.println("feed:" + feed);
    }
}
