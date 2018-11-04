/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.file;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.file.FileStreams;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.streams.BeaconStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class FileStreamsTest extends TestTopology {
    
    private static final class FileCreator implements Consumer<Long>, AutoCloseable {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        private final int repeat;
        private final String[] files;
        private final String dir;

        private FileCreator(String dir, int repeat, String[] files) {
            this.dir = dir;
            this.repeat = repeat;
            this.files = files;
        }
        
        @Override
        public void accept(Long arg0) {
            try {
                for (int r = 0; r < repeat; r++) {
                    for (int i = 0; i < files.length; i++) {
                        File newFile = new File(dir, files[i]);
                        if (repeat > 1) {
                            newFile.delete();
                            Thread.sleep(10);
                        }
                        Files.createFile(newFile.toPath());
                        Thread.sleep(10);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        
        @Override
        public void close() throws Exception {
            // Ensure we clean up!
            for (int i = 0; i < files.length; i++) {
                new File(files[i]).delete();
            }
        }
    }

    /**
     * Test that directory watcher creates the correct output.
     */
    @Test
    public void testDirectoryWatcherOrder() throws Exception {
        final Topology t = newTopology("testDirectoryWatcherOrder");
        runDirectoryWatcher(t, 20, 1);

    }
    @Test
    public void testDirectoryWatcherOrderWithDelete() throws Exception {
        final Topology t = newTopology("testDirectoryWatcherOrderWithDelete");
        runDirectoryWatcher(t, 20, 3);
    }
    
    private static String PREFIX = "ADIWQDWDMQ";
    
    private void runDirectoryWatcher(final Topology t, int numberOfFiles, int repeat) throws Exception {
        
        // final Path dir = Files.createTempDirectory("testdw");
        final String[] files = new String[numberOfFiles];
        for (int i = 0; i < files.length; i++) {
            files[i] = PREFIX + (numberOfFiles - i);
        }
        
        String dir;
        if (this.getTesterType() == Type.EMBEDDED_TESTER) {
            dir = Files.createTempDirectory("testdw").toAbsolutePath().toString();
        } else {
            dir = ".";
            this.getConfig().put(JobProperties.DATA_DIRECTORY, dir);
        }
        
        TStream<String> rawFileNames = FileStreams.directoryWatcher(t, dir);
        
        TStream<String> fileNames = rawFileNames.modify(fn -> new File(fn).getName());
        fileNames = fileNames.filter(fn -> fn.startsWith(PREFIX));
        //fileNames.print();

        // Create the files from within the topology to ensure it works
        // in distributed and standalone.
        //
        // Due to vagaries / delays that can occur in operator startup
        // in different processes (distributed mode), delay the initial
        // file creation process to give the watcher a chance to startup.
        //
        // e.g., with numberOfFiles=20 & repeat=1, each group of files
        // only lasts 20*(10ms*2) => 200ms.  That can easily happen before
        // the watcher is started and has done its first dir.listFiles(),
        // with the result being not seeing/processing the expected number
        // of files.
        
        TSink creator = addStartupDelay(BeaconStreams.single(t))
                    .forEach(createFiles(dir, files, repeat));
        
        creator.colocate(rawFileNames);

        Tester tester = t.getTester();
        Condition<Long> expectedCount = tester.tupleCount(fileNames,
                numberOfFiles * repeat);
        
        String[] expectedFileNames = files;
        if (repeat != 1) {
            expectedFileNames = new String[files.length * repeat];
            for (int r = 0; r < repeat; r++)
                System.arraycopy(files, 0, expectedFileNames, r * files.length, files.length);
        }
        Condition<List<String>> expectedNames = tester.stringContentsUnordered(
                fileNames.filter(x -> true), expectedFileNames);

        complete(tester, expectedCount, 60, TimeUnit.SECONDS);
        
        if (!".".equals(dir)) {
           // Files.d
        }

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedNames.toString(), expectedNames.valid());
    }

    static Consumer<Long> createFiles(String dir, final String[] files, final int repeat) {
        return new FileCreator(dir, repeat, files);
    }

    @Test
    public void testTextFileReader() throws Exception {
        
        Path tmpFile = Files.createTempFile("test", "txt");
        
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(tmpFile.toFile()), StandardCharsets.UTF_8));
        
        String[] lines = new String[] {
                "If you can keep your head when all about you",
                "Are losing theirs and blaming it on you,",
                "If you can trust yourself when all men doubt you,",
                "But make allowance for their doubting too;"                
        };
        
        for (int i = 0; i < lines.length; i++) {
            bw.write(lines[i]);
            bw.write("\n");
        }
        bw.flush();
        bw.close();
        
        final Topology t = new Topology("testTextFileReader");
        String fileLocation;
        if (getTesterType() == Type.EMBEDDED_TESTER) {
            fileLocation = tmpFile.toAbsolutePath().toString();
        } else {
            t.addFileDependency(tmpFile.toAbsolutePath().toString(), "etc");
            fileLocation = "etc/" + tmpFile.getFileName().toString();
        }
        
        TStream<String> fileName = t.strings(fileLocation);
        if (getTesterType() != Type.EMBEDDED_TESTER)
            fileName = fileName.modify(
                f -> new File(PERuntime.getPE().getApplicationDirectory(), f).getAbsolutePath());
        TStream<String> contents = FileStreams.textFileReader(fileName);
        
        
        Tester tester = t.getTester();
        Condition<Long> expectedCount = tester.tupleCount(contents, lines.length);
        Condition<List<String>> expectedContent = tester.stringContents(
                contents, lines);

        complete(tester, expectedCount, 10, TimeUnit.SECONDS);

        assertTrue(expectedCount.toString(), expectedCount.valid());
        assertTrue(expectedContent.toString(), expectedContent.valid());
        
        tmpFile.toFile().delete();
    }
}
