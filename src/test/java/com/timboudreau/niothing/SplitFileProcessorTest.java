package com.timboudreau.niothing;

import com.mastfrog.util.Streams;
import com.timboudreau.niothing.SplitFileProcessor.InputStreamProcessor;
import com.timboudreau.niothing.SplitFileProcessor.RegionController;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitFileProcessorTest {

    static File file;

    static Set<String> lines = new HashSet<>();

    static int lineLength;

    @BeforeClass
    public static void setUpClass() throws IOException {
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        file = new File(tmp, SplitFileProcessorTest.class.getName() + "_" + System.currentTimeMillis());
        if (!file.createNewFile()) {
            throw new IOException("Could not create " + file);
        }
        try (PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)))) {
            for (char region = 'A'; region < 'z'; region++) {
                StringBuilder sb = new StringBuilder();
                sb.append("Region " + region + ":");
                for (int i = 0; i < 10; i++) {
                    sb.append("abcdefghijklmnopqrstuvwxyz-" + i);
                }
                sb.append('\n');
                int len = sb.length();
                if (lineLength != 0) {
                    assertEquals("Test is broken - line length not stable", len, lineLength);
                }
                lineLength = len;
                lines.add(sb.toString());
                out.print(sb.toString());
            }
        }
    }

    @AfterClass
    public static void tearDownClass() {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testProcess() throws InterruptedException {
        List<Region> regions = new ArrayList<Region>(10);
        int pos = 0;
        for (char regionId = 'A'; regionId < 'z'; regionId++) {
            Region region = new Region(pos, pos + lineLength);
            pos += lineLength;
            regions.add(region);
        }
        List<RP> processors = new ArrayList<>(3);
        processors.add(new RP());
        processors.add(new RP());
        processors.add(new RP());
        RC controller = new RC();
        ExecutorService threadPool = Executors.newFixedThreadPool(5);
        SplitFileProcessor.process(file, regions, controller, threadPool, processors);
        controller.await();
        assertEquals(lines, controller.all);
        assertTrue(controller.onDoneCalled);
    }

    static int processorIds;
    
    static Random random = new Random(122);

    static class RP implements InputStreamProcessor<String> {

        final int id = processorIds++;
        
        @Override
        public String process(RegionInputStream chunk) throws IOException {
            // Without this sleep, the other threads will not be started
            try {
                // Intentionally introduce some jitter here, and ensure other
                // threads are not starved so we get some real concurrency
                Thread.sleep(10 + random.nextInt(50));
            } catch (InterruptedException ex) {
                Logger.getLogger(SplitFileProcessorTest.class.getName()).log(Level.SEVERE, null, ex);
            }
            String result = Streams.readString(chunk);
            return result;
        }

        public String toString() {
            return "RP-" + id;
        }
    }

    static class RC implements RegionController<String> {

        Set<String> all = Collections.<String>synchronizedSet(new HashSet<String>());
        private final RuntimeException errorHolder = new RuntimeException("Exceptions thrown from process()");
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile boolean thrown;
        volatile boolean onDoneCalled;

        @Override
        public void onOneDone(Region region, String result) {
            all.add(result);
        }

        @Override
        public void onException(Region region, Exception exception, InputStreamProcessor processorThatFailed) {
            errorHolder.addSuppressed(exception);
            thrown = true;
        }

        @Override
        public void onAllDone() {
            latch.countDown();
            onDoneCalled = true;
        }

        void await() throws InterruptedException {
            latch.await();
            if (thrown) {
                throw errorHolder;
            }
        }
    }
}
