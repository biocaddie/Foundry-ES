package org.neuinfo.foundry.common.util;

import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 12/15/15.
 */
public class DataParserFactory {
    Map<String, String> parserName2ClassQNMap = new ConcurrentHashMap<String, String>();
    private static DataParserFactory instance;

    private DataParserFactory() {
        parserName2ClassQNMap.put("geo", "org.neuinfo.foundry.common.util.GEOParser");
        parserName2ClassQNMap.put("rss", "org.neuinfo.foundry.common.util.RSSFeedParser");
        parserName2ClassQNMap.put("atom", "org.neuinfo.foundry.common.util.AtomFeedParser");
    }

    public static synchronized DataParserFactory getInstance() {
        if (instance == null) {
            instance = new DataParserFactory();
        }
        return instance;
    }

    public void registerParser(String name, String classQN) {
        parserName2ClassQNMap.put(name, classQN);
    }

    public IDataParser createDataParser(String parserName, InputDataIterator iter) throws Exception {
        String qn = parserName2ClassQNMap.get(parserName);
        if (qn == null) {
            throw new RuntimeException("Unknown parser type:" + parserName);
        }
        IDataParser parser = (IDataParser) Class.forName(qn).newInstance();
        parser.initialize(iter);
        return parser;
    }
}
