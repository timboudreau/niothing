/*
 * The MIT License
 *
 * Copyright 2015 Tim Boudreau.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.timboudreau.niothing;

import com.mastfrog.util.Exceptions;
import com.mastfrog.util.Streams;
import com.mastfrog.util.collections.CollectionUtils;
import com.mastfrog.util.collections.Converter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe Iterable which produces an iterator of InputStreams of regions of
 * a single file, which can be accessed concurrently. Memory-maps the total
 * region requested, and returns InputStreams over sub-ByteBuffers representing
 * each region.
 *
 * @author Tim Boudreau
 */
public final class SplitFile implements Iterable<RegionInputStream>, AutoCloseable {

    private final List<Region> regions;
    private final File file;
    private FileChannel channel;
    private MappedByteBuffer allRegions;

    public SplitFile(File file, Collection<Region> regions) {
        this.file = file;
        this.regions = new ArrayList<>(regions);
        Collections.sort(this.regions);
        // Trigger ISE early if total region to map is > Integer.MAX_VALUE
        // This can be avoided if we want to map individual regions instead of
        // the entire file - but the OS's memory manager will generally take
        // care of not actually slurping the whole file into memory instantly
        range();
    }

    private Region first() {
        return regions.get(0);
    }

    private Region last() {
        return regions.get(regions.size() - 1);
    }

    private Region range() {
        // This will throw an exception if the total range to map is > Integer.MAX_VALUE
        // This could be avoided by mapping individual regions if needed, but likely
        // not to perform as well
        return new Region(first().start, last().end);
    }

    synchronized MappedByteBuffer masterBuffer() throws IOException {
        if (channel == null) {
            channel = new FileInputStream(file).getChannel();
            Region range = range();
            allRegions = channel.map(FileChannel.MapMode.READ_ONLY, range.start, range.length());
        }
        return allRegions;
    }

    @Override
    public Iterator<RegionInputStream> iterator() {
        if (regions.isEmpty()) {
            return Collections.emptyIterator();
        }
        synchronized (this) {
            if (channel != null) {
                // Two iterators could result in the channel being closed while still in
                // use by another iterator
                throw new IllegalStateException("Cannot reuse this object until all streams "
                        + "from the previously returned iterator are closed.");
            }
        }
        return new SynchronizedIterator<>(CollectionUtils.convertedIterator(new RegionToInputStream(), regions.iterator()));
    }

    @Override
    public void close() throws Exception {
        FileChannel openFileChannel;
        synchronized (this) {
            openFileChannel = channel;
            channel = null;
            allRegions = null;
        }
        if (openFileChannel != null) {
            openFileChannel.close();
        }
    }

    private ByteBuffer bufferFor(Region region) throws IOException {
        ByteBuffer master = masterBuffer();
        master.position((int) region.start);
        ByteBuffer result = master.slice();
        result.limit(region.length());
        return result;
    }

    @Override
    public String toString() {
        return "SplitFile{" + file + ", " + regions + "}";
    }

    private final class RegionToInputStream implements Converter<RegionInputStream, Region> {

        private final AtomicInteger counter = new AtomicInteger(regions.size());

        @Override
        public RegionInputStream convert(Region region) {
            try {
                ByteBuffer buf = bufferFor(region);
                return new CloseCountingInputStream(region, Streams.asInputStream(buf), counter);
            } catch (IOException ex) {
                return Exceptions.chuck(ex);
            }
        }

        @Override
        public Region unconvert(RegionInputStream t) {
            throw new UnsupportedOperationException("Not needed.");
        }
    }

    private void onLastStreamClosed() {
        try {
            close();
        } catch (Exception ex) {
            Exceptions.chuck(ex);
        }
    }
    
    private final class CloseCountingInputStream extends RegionInputStream {

        final AtomicInteger closeCounter;
        private boolean closed;

        public CloseCountingInputStream(Region region, InputStream streamToWrap, AtomicInteger closeCounter) {
            super(region, streamToWrap);
            this.closeCounter = closeCounter;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    super.close();
                } finally {
                    if (0 == closeCounter.decrementAndGet()) {
                        onLastStreamClosed();
                    }
                }
            }
        }
    }

    /**
     * Multiple threads will be banging on this iterator at once, so it needs to
     * be synchronized, though the underlying collection doesn't.
     *
     * @param <T> The type
     */
    static class SynchronizedIterator<T> implements Iterator<T> {

        private final Iterator<T> iterator;

        public SynchronizedIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public synchronized boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public synchronized T next() {
            return iterator.next();
        }
    }
}
