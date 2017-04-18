package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.neuinfo.foundry.common.ingestion.FileType;
import org.neuinfo.foundry.common.ingestion.FormatType;
import org.neuinfo.foundry.common.ingestion.IngestCommandInfo;
import org.neuinfo.foundry.common.ingestion.IngestionLanguageInterpreter;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.IngestorIterable;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 2/23/17.
 */
public class ScriptedIngestor implements Ingestor {
    Map<String, String> optionMap;
    List<IngestCommandInfo> commandInfos;
    IngestorIterable theCursor;
    //FIXME for debugging
    boolean useCache = true;
    Map<String, IngestCommandInfo> cmdMap = new HashMap<String, IngestCommandInfo>();

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        String script = options.get("script");
        IngestionLanguageInterpreter interpreter = new IngestionLanguageInterpreter();
        interpreter.parse(script);
        this.commandInfos = interpreter.getCommandInfos();
        for (IngestCommandInfo ici : commandInfos) {
            if (!ici.getCommandName().equals("ingest")) {
                cmdMap.put(ici.getAlias(), ici);
            }
        }
    }

    @Override
    public void startup() throws Exception {
        List<IngestCommandInfo> downloadCommands = filter(IngestCommandInfo.DOWNLOAD, null);
        List<PathWrapper> downloadList = new ArrayList<PathWrapper>(downloadCommands.size());
        List<IngestCommandInfo> onDemandDownloads = new ArrayList<IngestCommandInfo>(downloadCommands.size());
        for (IngestCommandInfo ici : downloadCommands) {
            if (!IngestorHelper.isParametrized(ici.getPath())) {
                PathWrapper pw = handleDownload(ici);
                downloadList.add(pw);
            } else {
                onDemandDownloads.add(ici);
            }
        }
        List<PathWrapper> allExtractedFiles = new ArrayList<PathWrapper>();
        for (PathWrapper bundlePath : downloadList) {
            List<IngestCommandInfo> extractList = filter(IngestCommandInfo.EXTRACT, bundlePath.alias);
            List<PathWrapper> pwList = extractFiles(extractList, bundlePath);
            if (!pwList.isEmpty()) {
                allExtractedFiles.addAll(pwList);
            }
        }
        // no extraction
        if (allExtractedFiles.isEmpty()) {
            allExtractedFiles.addAll(downloadList);
        }

        List<IngestorIterable> cursors = new ArrayList<IngestorIterable>(allExtractedFiles.size());

        Map<String, IngestorIterable> cursorMap = new HashMap<String, IngestorIterable>();
        for (PathWrapper source : allExtractedFiles) {
            IngestCommandInfo ici = cmdMap.get(source.alias);
            IngestorIterable cursor = prepCursor(source, ici);
            cursors.add(cursor);
            cursorMap.put(source.alias, cursor);
        }
        // handle on demand (parametrized) downloads
        for (IngestCommandInfo ici : onDemandDownloads) {
            IngestorIterable cursor = prepOnDemandCursor(ici);
            cursors.add(cursor);
            cursorMap.put(ici.getAlias(), cursor);
        }
        List<IngestCommandInfo> joins = orderJoins();
        if (joins.isEmpty()) {
            Assertion.assertTrue(cursors.size() == 1);
            theCursor = cursors.get(0);
        } else {
            LinkedHashMap<String, Joinable> joinCursorMap = new LinkedHashMap<String, Joinable>(7);
            String prevRightAlias = null;
            for (IngestCommandInfo ici : joins) {
                IngestorIterable cursor;
                if (prevRightAlias == null) {
                    cursor = cursorMap.get(ici.getLeft().getAlias());
                    Assertion.assertNotNull(cursor);
                    joinCursorMap.put(ici.getLeft().getAlias(), (Joinable) cursor);
                } else {
                    Assertion.assertTrue(ici.getLeft().getAlias().equals(prevRightAlias));
                }
                cursor = cursorMap.get(ici.getRight().getAlias());
                Assertion.assertNotNull(cursor);
                joinCursorMap.put(ici.getRight().getAlias(), (Joinable) cursor);
                prevRightAlias = ici.getRight().getAlias();
            }
            JoinCursor joinCursor = new JoinCursor(joins, joinCursorMap);
            joinCursor.startup();
            theCursor = joinCursor;
        }

    }

    @Override
    public Result prepPayload() {
        return this.theCursor.prepPayload();
    }

    @Override
    public String getName() {
        return "ScriptedIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean hasNext() {
        return theCursor != null && theCursor.hasNext();
    }

    List<IngestCommandInfo> filter(String commandName, String fromAlias) {
        List<IngestCommandInfo> filteredList = new ArrayList<IngestCommandInfo>(commandInfos.size());
        for (IngestCommandInfo ici : commandInfos) {
            if (ici.getCommandName().equals(commandName)) {
                if (fromAlias != null) {
                    if (fromAlias.equals(ici.getFromAlias())) {
                        filteredList.add(ici);
                    }
                } else {
                    filteredList.add(ici);
                }
            }
        }
        return filteredList;
    }

    List<IngestCommandInfo> orderJoins() {
        List<IngestCommandInfo> joins = new ArrayList<IngestCommandInfo>(commandInfos.size());
        for (IngestCommandInfo ici : commandInfos) {
            if (ici.getCommandName().equals(IngestCommandInfo.JOIN)) {
                joins.add(ici);
            }
        }
        if (!joins.isEmpty()) {
            IngestCommandInfo head = null;
            for (IngestCommandInfo ici : joins) {
                boolean found = false;
                for (IngestCommandInfo ici2 : joins) {
                    if (ici != ici2) {
                        if (ici.getLeft() == ici2.getLeft() || ici.getLeft() == ici2.getRight()) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    head = ici;
                    break;
                }
            }
            Assertion.assertNotNull(head);
            List<IngestCommandInfo> orderedJoins = new ArrayList<IngestCommandInfo>(joins.size());
            orderedJoins.add(head);
            joins.remove(head);
            IngestCommandInfo n = head;
            while (!joins.isEmpty()) {
                boolean found = false;
                for (Iterator<IngestCommandInfo> iterator = joins.iterator(); iterator.hasNext(); ) {
                    IngestCommandInfo ici = iterator.next();
                    if (n.getRight() == ici.getLeft()) {
                        orderedJoins.add(ici);
                        n = ici;
                        iterator.remove();
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }
            return orderedJoins;
        }
        return joins;
    }

    PathWrapper handleDownload(IngestCommandInfo ici) throws Exception {
        File file = ContentLoader.getContent(ici.getPath(), null, useCache, ici.getUser(), ici.getPassword());
        PathWrapper pw = new PathWrapper(file.getAbsolutePath(), ici.getAlias());
        return pw;
    }


    List<PathWrapper> extractFiles(List<IngestCommandInfo> iciList, PathWrapper bundlePath) throws Exception {
        if (iciList.isEmpty()) {
            return Collections.emptyList();
        }
        Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");
        String cacheDirName = new File(bundlePath.path).getName().replaceAll("[\\./\\(\\)]", "_") + "_files";
        File destDir = new File(cacheRoot, cacheDirName);
        destDir.mkdir();
        List<String> fileNames2Match = new ArrayList<String>(iciList.size());
        Map<String, IngestCommandInfo> fileName2ICIMap = new HashMap<String, IngestCommandInfo>(11);
        for (IngestCommandInfo ici : iciList) {
            fileNames2Match.add(ici.getPath());
            fileName2ICIMap.put(ici.getPath(), ici);
        }

        List<File> files = Utils.extractFilesFromTar(bundlePath.path, fileNames2Match, destDir);
        List<PathWrapper> pwList = new ArrayList<PathWrapper>(files.size());
        for (File f : files) {
            IngestCommandInfo ici = fileName2ICIMap.get(f.getName());
            Assertion.assertNotNull(ici);
            pwList.add(new PathWrapper(f.getAbsolutePath(), ici.getAlias()));
        }

        return pwList;
    }

    IngestorIterable prepOnDemandCursor(IngestCommandInfo ici) throws Exception {
        Assertion.assertTrue(IngestorHelper.isParametrized(ici.getPath()));
        Map<String, String> options = new HashMap<String, String>();
        if (ici.getParamMap() != null) {
            options = ici.getParamMap();
        }
        if (ici.getFileType() == FileType.XML) {
            options.put("documentElement", ici.getDocElement());
            options.put("topElement", ici.getTopElement());
            XMLCursor cursor = new XMLCursor(ici.getAlias(), ici.getUser(), ici.getPassword(), ici.getPath());
            cursor.initialize(options);
            cursor.startup();
            return cursor;
        } else {
            throw new UnsupportedOperationException("Only XML cursor supports on demand (parameterized) web access");
        }

    }

    IngestorIterable prepCursor(PathWrapper source, IngestCommandInfo ici) throws Exception {
        Map<String, String> options = new HashMap<String, String>();
        if (ici.getParamMap() != null) {
            options = ici.getParamMap();
        }
        if (ici.getFileType() == FileType.XML) {
            options.put("documentElement", ici.getDocElement());
            if (ici.getTopElement() != null) {
                options.put("topElement", ici.getTopElement());
            }
            XMLCursor cursor = new XMLCursor(ici.getAlias(), new File(source.path));

            cursor.initialize(options);
            cursor.startup();
            return cursor;
        } else if (ici.getFileType() == FileType.CSV) {
            CSVCursor cursor = new CSVCursor(ici.getAlias(), new File(source.path));
            cursor.initialize(options);
            cursor.startup();
            return cursor;
        }
        //TODO JSON cursor
        return null;
    }


    public static class PathWrapper {
        final String path;
        final String alias;
        FileType fileType;
        FormatType formatType;

        public PathWrapper(String path, String alias) {
            this.path = path;
            this.alias = alias;
        }
    }
}
