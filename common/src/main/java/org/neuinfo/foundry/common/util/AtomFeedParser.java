package org.neuinfo.foundry.common.util;

import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;

import java.io.StringReader;

/**
 * Created by bozyurt on 12/14/15.
 */
public class AtomFeedParser implements IDataParser {
    protected SyndFeed feed;

    public AtomFeedParser() {
    }

    @Override
    public void initialize(InputDataIterator iterator) throws Exception {
        String atomContent = iterator.getContent();
        SyndFeedInput input = new SyndFeedInput();
        this.feed = input.build(new StringReader(atomContent));
    }

    public JSONObject toJSON() {
        //TODO
        return null;
    }
}
