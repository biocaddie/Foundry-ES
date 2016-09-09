package org.neuinfo.foundry.common.transform;

import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * Created by bozyurt on 5/4/15.
 */
public class TransformMappingUtils {


    public static String loadTransformMappingScript(String scriptPath) {
        File f = new File(scriptPath);
        if (f.isFile()) {
            try {
                return Utils.loadAsString(scriptPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            return loadTransformMappingScriptFromClasspath(scriptPath);
        }
        return null;
    }

    public static String loadTransformMappingScriptFromClasspath(String scriptPath) {
        InputStream in = null;
        try {
            in = Transformation.class.getClassLoader().getResourceAsStream(scriptPath);
            Scanner scanner = new Scanner(in).useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        } finally {
            Utils.close(in);
        }
    }

}
