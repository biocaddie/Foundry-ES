package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.InputDataIterator;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 10/29/14.
 * FIXME needs maxDocs, testMode support
 */
public class RSSIngestor implements Ingestor {
    private Map<String, String> optionMap;
    private String ingestURL;
    private boolean allowDuplicates = false;
    private Iterator<RSSFeedParser.RSSItem> rssItemIterator;
    private int numRecs = -1;
    boolean sampleMode = false;
    int sampleSize = 1;
    static Logger log = Logger.getLogger(RSSIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.allowDuplicates = options.containsKey("allowDuplicates") ?
                Boolean.parseBoolean(options.get("allowDuplicates")) : false;
        this.ingestURL = options.get("ingestURL");
        if (options.containsKey("sampleMode")) {
            this.sampleMode = Boolean.parseBoolean(options.get("sampleMode"));
        }
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        Assertion.assertNotNull(this.ingestURL);
    }

    @Override
    public void startup() throws Exception {
        InputDataIterator iter = new InputDataIterator(new URL(this.ingestURL));
        RSSFeedParser parser = (RSSFeedParser) DataParserFactory.getInstance().createDataParser("rss", iter);
        // RSSFeedParser parser = new RSSFeedParser(this.ingestURL);

        RSSFeedParser.RSSFeed feed = parser.getFeed();
        List<RSSFeedParser.RSSItem> items = feed.getItems();
        if (sampleMode) {
            items = items.subList(0,sampleSize);
        }
        this.rssItemIterator = items.iterator();
        this.numRecs = items.size();
    }

    @Override
    public Result prepPayload() {
        try {
            RSSFeedParser.RSSItem rssItem = rssItemIterator.next();
            JSONObject json = new JSONObject();
            json.put("guid", rssItem.getGuid());
            json.put("title", rssItem.getTitle());
            json.put("link", rssItem.getLink());
            json.put("description", rssItem.getDescription());
            JSONUtils.add2JSON(json, "author", rssItem.getAuthor());
            JSONUtils.add2JSON(json, "comments", rssItem.getComments());
            SimpleDateFormat sdf = new SimpleDateFormat(" yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

            JSONUtils.add2JSON(json, "pubDate", sdf.format(rssItem.getPubDate()));
            JSONArray jsArr = new JSONArray();
            if (rssItem.getCategories() != null && !rssItem.getCategories().isEmpty()) {
                for (RSSFeedParser.RSSCategory cat : rssItem.getCategories()) {
                    jsArr.put(cat.getCategory());
                }
            }
            json.put("categories", jsArr);

            return new Result(json, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            log.error("prepPayload",t);
            t.printStackTrace();
            return new Result(null, Result.Status.ERROR, t.getMessage());
        }
    }

    @Override
    public String getName() {
        return "RSSIngestor";
    }

    @Override
    public int getNumRecords() {
        return this.numRecs;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {
        // no op
    }

    @Override
    public boolean hasNext() {
        return rssItemIterator.hasNext();
    }
}
