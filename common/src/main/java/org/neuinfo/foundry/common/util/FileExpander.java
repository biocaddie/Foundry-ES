package org.neuinfo.foundry.common.util;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Iterator;
import java.util.jar.Pack200;

/**
 * Created by bozyurt on 12/17/15.
 */
public class FileExpander {
    TikaConfig tika;

    public FileExpander() throws TikaException, IOException {
        this.tika = new TikaConfig();
    }

    public File expandIfNecessary(File inFile, boolean useCache) throws Exception {
        String fileType = determineFileType(inFile);
        if (fileType.equals("zip")) {
            File destDir = Utils.fromFile2Dir(inFile);
            if (useCache && destDir.isDirectory()) {
                // check if there are any files
                String[] list = destDir.list();
                if (list.length > 0) {
                    return destDir;
                }
            }
            destDir.mkdir();

            Unzipper unzipper = new Unzipper(inFile, destDir);
            unzipper.expand();
            return destDir;
        } else if (fileType.equals("gzip")) {
            if (inFile.getName().endsWith(".tgz") || inFile.getName().endsWith(".tar.gz")) {
                File destDir = Utils.fromFile2Dir(inFile);
                destDir.mkdir();
                Unpacker unpacker = new Unpacker(inFile, destDir);
                unpacker.unpack();
                return destDir;
            } else {
                String destFilename = inFile.getName().replaceAll("\\.[^\\.]+$","");
                File destFile = new File(inFile.getParentFile(), destFilename);
                Utils.extractGzippedFile(inFile.getAbsolutePath(), destFile);
                Assertion.assertTrue(destFile.isFile());
                return destFile;
            }
        }
        return inFile;
    }

    public String determineFileType(File aFile) throws IOException {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, aFile.toString());
        Path path  = Paths.get(aFile.toURI());
        MediaType mediaType = tika.getDetector().detect(TikaInputStream.get(path), metadata);
        System.out.println(mediaType.toString());
        return mediaType.getSubtype();
    }


    public static void main(String[] args) throws Exception {
        FileExpander expander = new FileExpander();


        // String type = expander.determineFileType(new File("/var/data/clinicaltrials_gov"));
        String type = expander.determineFileType(new File("/home/bozyurt/Foundry-ES.tgz"));
        System.out.println("type:" + type);

        File destDir = expander.expandIfNecessary(new File("/var/data/clinicaltrials_gov"), false);
        System.out.println("destDir:" + destDir);
    }
}
