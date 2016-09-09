package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.util.Utils;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by bozyurt on 12/22/15.
 */
public class Parameters {
    Properties props;
    private static Parameters ourInstance = new Parameters();

    public static Parameters getInstance() {
        return ourInstance;
    }

    private Parameters() {
        try {
            this.props = Utils.loadProperties("consumer.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getParam(String paramName) {
        return this.props.getProperty(paramName);
    }
}
