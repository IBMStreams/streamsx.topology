/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.file;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.types.RString;

@PrimitiveOperator
@OutputPortSet(cardinality = 1)
public class DirectoryWatcher extends ProcessTupleProducer implements
        FileFilter {

    private String directory;
    private File dirFile;

    private final Set<String> seenFiles = new HashSet<>();

    protected String getDirectory() {
        return directory;
    }

    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        super.initialize(context);
        dirFile = new File(getDirectory());
        if (!dirFile.isAbsolute())
            dirFile = new File(getOperatorContext().getPE().getDataDirectory(),
                    getDirectory());
        
        dirFile = dirFile.getCanonicalFile();
    }

    @Parameter
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    protected void sortAndSubmit(List<File> files) throws Exception {

        if (files.size() > 1) {
            Collections.sort(files, new Comparator<File>() {

                @Override
                public int compare(File o1, File o2) {
                    return Long.compare(o1.lastModified(), o2.lastModified());
                }
            });
        }

        for (File file : files) {
            if (accept(file)) {
                getOutput(0).submitAsTuple(new RString(file.getAbsolutePath()));
                seenFiles.add(file.getName());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void process() throws Exception {

        Path dir = dirFile.toPath();

        WatchService watcher = FileSystems.getDefault().newWatchService();
        dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE);

        sortAndSubmit(Arrays.asList(dirFile.listFiles(this)));

        for (;!Thread.interrupted();) {
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException e) {
                // shutdown has been requested
                return;
            }

            List<File> newFiles = new ArrayList<>();
            boolean needFullScan = false;
            for (WatchEvent<?> watchEvent : key.pollEvents()) {

                if (ENTRY_CREATE == watchEvent.kind()) {
                    Path newPath = ((WatchEvent<Path>) watchEvent).context();
                    File newFile = toFile(newPath);
                    if (accept(newFile))
                        newFiles.add(newFile);
                } else if (ENTRY_DELETE == watchEvent.kind()) {
                    Path deletedPath = ((WatchEvent<Path>) watchEvent)
                            .context();
                    File deletedFile = toFile(deletedPath);
                    seenFiles.remove(deletedFile.getName());
                } else if (OVERFLOW == watchEvent.kind()) {
                    needFullScan = true;
                }
            }
            key.reset();

            if (needFullScan) {
                Collections.addAll(newFiles, dirFile.listFiles(this));
            }
            sortAndSubmit(newFiles);
        }
    }

    private File toFile(Path path) {
        if (path.isAbsolute())
            return path.toFile();
        return new File(dirFile, path.getFileName().toString());
    }

    @Override
    public boolean accept(File pathname) {
        return !seenFiles.contains(pathname.getName());
    }
}
