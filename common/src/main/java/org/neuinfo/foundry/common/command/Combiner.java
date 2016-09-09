package org.neuinfo.foundry.common.command;

import org.jdom2.Element;
import org.neuinfo.foundry.common.model.CommandOutput;
import org.neuinfo.foundry.common.model.ICommand;
import org.neuinfo.foundry.common.model.ICommandInput;
import org.neuinfo.foundry.common.model.ICommandOutput;
import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Created by bozyurt on 2/9/16.
 */
public class Combiner implements ICommand {

    @Override
    public ICommandOutput handle(ICommandInput input) {
        String surroundingTag = input.getParam("surroundingTag");
        List<File> inputFiles = input.getFiles();
        UUID uuid = UUID.randomUUID();
        File outFile = new File(inputFiles.get(0).getParentFile(), uuid.toString() + ".xmlOut");
        if (surroundingTag != null) {
            try {
                Element rootEl = new Element(surroundingTag);
                for (File f : inputFiles) {
                    Element docEl = Utils.loadXML(f.getAbsolutePath());

                    rootEl.addContent(docEl.clone());
                }
                Utils.saveXML(rootEl, outFile.getAbsolutePath());
                return new CommandOutput(Arrays.asList(outFile));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // just concatenate files
            BufferedWriter out = null;
            try {
                out = Utils.newUTF8CharSetWriter(outFile.getAbsolutePath());
                for (File f : inputFiles) {
                    BufferedReader in = null;
                    try {
                        in = Utils.newUTF8CharSetReader(f.getAbsolutePath());
                        String line;
                        while ((line = in.readLine()) != null) {
                            out.write(line);
                            out.newLine();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        Utils.close(in);
                    }
                }
                return new CommandOutput(Arrays.asList(outFile));
            } catch (IOException x) {
                x.printStackTrace();
            } finally {
                Utils.close(out);
            }
        }
        return null;
    }
}
