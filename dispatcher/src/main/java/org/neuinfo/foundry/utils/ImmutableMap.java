package org.neuinfo.foundry.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 4/4/14.
 */
public class ImmutableMap {
    Map<String,Object> map = new HashMap<String, Object>();

    public ImmutableMap put(String key, Object value) {
        map.put(key, value);
        return this;
    }

    public static ImmutableMap builder() {
        return new ImmutableMap();
    }

    public Map<String,Object> build() {
        return Collections.unmodifiableMap(map);
    }

}
