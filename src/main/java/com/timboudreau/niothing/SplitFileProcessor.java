package com.timboudreau.niothing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simplifies use of SplitFile - simply pass in a file, a list of regions, some
 * callbacks to process the input streams and a controller to pass results to,
 * and it will concurrently process a file. Concurrency is determined by the
 * number of processors passed in and the number of threads in the thread pool.
 * <p>
 * For concurrently processing regions of a single file and collecting the results.
 *
 * @author Tim Boudreau
 */
public final class SplitFileProcessor<T> {

    private final SplitFile splitFile;
    private final ExecutorService threadPool;
    private final Collection<? extends InputStreamProcessor<T>> processors;
    private final Iterator<RegionInputStream> streams;
    private final RegionController<T> controller;
    private final AtomicInteger activeCount = new AtomicInteger();

    private SplitFileProcessor(ExecutorService threadPool, SplitFile split, Collection<? extends InputStreamProcessor<T>> processors, RegionController<T> controller) {
        this.splitFile = split;
        this.threadPool = threadPool;
        this.controller = controller;
        streams = splitFile.iterator();
        this.processors = processors;
    }

    /**
     * Process a file as a series of regions which are concurrently passed as separate
     * input streams to the processors provided by the factory.
     *
     * @param <T> The return type of the InputStreamProcessors' process() method
     * - each result will be passed to the controller as it is computed
     * @param file The file
     * @param regions The regions of the file to process
     * @param controller The controller which will receive results
     * @param threadPool The thread pool to execute in
     * @param processorFactory Factory for InputStreamProcessors which specifies how many to use
     */
    public static <T> void process(File file, Collection<Region> regions, RegionController<T> controller, ExecutorService threadPool, InputStreamProcessorFactory<T> processorFactory) {
        process(file, regions, controller, threadPool, processorFactory.populate());
    }

    /**
     * Process a file as a series of regions which are concurrently passed as separate
     * input streams to the processors provided by the factory.
     *
     * @param <T> The return type of the InputStreamProcessors' process() method
     * - each result will be passed to the controller as it is computed
     * @param file The file
     * @param regions The regions of the file to process
     * @param controller The controller which will receive results
     * @param threadPool The thread pool to execute in
     * @param processors Workers which will be called with input streams
     * representing regions of the file
     */
    public static <T> void process(File file, Collection<Region> regions, RegionController<T> controller, ExecutorService threadPool, Collection<? extends InputStreamProcessor<T>> processors) {
        if (processors.isEmpty()) {
            throw new IllegalArgumentException("Empty collection of processors - can't do anything");
        }
        if (regions.isEmpty()) {
            controller.onAllDone();
            return;
        }
        SplitFileProcessor<T> processor = new SplitFileProcessor<T>(threadPool, new SplitFile(file, regions), processors, controller);
        processor.start();
    }

    private void start() {
        for (InputStreamProcessor<T> processor : processors) {
            threadPool.submit(new ChunkProcessorRunner(processor));
        }
    }

    private class ChunkProcessorRunner implements Runnable {

        private final InputStreamProcessor<T> processor;

        private ChunkProcessorRunner(InputStreamProcessor<T> processor) {
            this.processor = processor;
        }

        @Override
        public void run() {
            activeCount.incrementAndGet();
            try {
                for (;;) {
                    RegionInputStream stream = null;
                    // Synchronize to ensure that another thread doesn't remove the next
                    // element between the call to hasNext() and next()
                    synchronized (streams) {
                        if (streams.hasNext()) {
                            stream = streams.next();
                        }
                    }
                    if (stream != null) {
                        try {
                            T result = processor.process(stream);
                            controller.onOneDone(stream.region(), result);
                            if (Thread.currentThread().interrupted()) {
                                // ExecutorService.shutdown(true) was called
                                break;
                            }
                        } catch (Exception e) {
                            try {
                                controller.onException(stream.region(), e, processor);
                            } catch (Exception ex) {
                                // If onException throws an exception, we're kind of hosed,
                                // but don't completely swallow it
                                Logger.getLogger(SplitFileProcessor.class.getName()).log(Level.SEVERE, "controller.onException() threw an exception, aborting", ex);
                                break;
                            }
                        } finally {
                            try {
                                stream.close();
                            } catch (IOException ex) {
                                Logger.getLogger(SplitFileProcessor.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            if (!streams.hasNext()) {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                // Ensure we only call onAllDone() when the last thread exits
                if (activeCount.decrementAndGet() == 0) {
                    try {
                        controller.onAllDone();
                    } finally {
                        try {
                            splitFile.close();
                        } catch (Exception ex) {
                            Logger.getLogger(SplitFileProcessor.class.getName()).log(Level.SEVERE, "Exception closing " + splitFile, ex);
                        }
                    }
                }
            }
        }
    }

    /**
     * Callable-like worker object which is handed InputStreams and processes
     * them. Will not be called concurrently, but may be called multiple times
     * serially.
     *
     * @param <T> The type of result to return - use Void if none
     */
    public interface InputStreamProcessor<T> {

        /**
         * Process an input stream representing one region of a file
         *
         * @param chunk The input stream
         * @return Some result of processing the ChunkController will
         * understand, or null if none is needed
         * @throws IOException if something goes wrong
         */
        T process(RegionInputStream chunk) throws IOException;
    }

    /**
     * Optional factory for input stream processors, which may be less verbose
     * than populating a list.
     *
     * @param <T> The return type of the input stream processors
     */
    public static abstract class InputStreamProcessorFactory<T> {

        private final int count;

        /**
         * Create a new factory which expects create() to be called count number
         * of times
         *
         * @param count The number of processors to create / the maximum
         * concurrency of processing
         */
        public InputStreamProcessorFactory(int count) {
            if (count <= 0) {
                throw new IllegalArgumentException("Count must be >= 1");
            }
            this.count = count;
        }

        public abstract InputStreamProcessor<T> create();

        List<InputStreamProcessor<T>> populate() {
            List<InputStreamProcessor<T>> result = new ArrayList(count);
            for (int i = 0; i < count; i++) {
                result.add(create());
            }
            return result;
        }
    }

    /**
     * Objects which is given results of processing file regions.
     *
     * @param <T> The return type for the InputStreamProcecssors process()
     * method.
     */
    public interface RegionController<T> {

        /**
         * Called when any thread has completed processing one region, with the
         * result. Must be thread-safe.
         *
         * @param region The region that was processed
         * @param result The result of processing
         */
        void onOneDone(Region region, T result);

        /**
         * Called if an exception is thrown by the process() method of an
         * InputStreamProcessor. Must be thread-safe.
         *
         * @param region The region
         * @param exception WHat went wrong
         * @param processorThatFailed The processor that threw the exception
         */
        void onException(Region region, Exception exception, InputStreamProcessor<T> processorThatFailed);

        /**
         * Called when all regions of the file have been processed and
         * onOneDone() has been called for every result.
         */
        void onAllDone();
    }
}
