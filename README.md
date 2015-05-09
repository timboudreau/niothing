NioThing
========

The answer to a little challenge presented to me by Jon Locke:  *Say you have a very large file, 
(say, map data) with well-defined sections you know the offsets of a priori, and you want to 
parallelize processing those sections, and you need to pass that to foreign code that expects
an `InputStream` for each region*.

This is a little library to make that specific task easy.  It comes in two layers:

 * You can just use `SplitFile` and get an `Iterator<InputStream>`
    * Minor caveat - if you call the iterator concurrently, synchronize on it so another thread doesn't
remove the element you're about to fetch before you can call `next()`
 * You can use `SplitFileProcessor.process()`, implement a few tiny classes and pass it 
a file, some regions and a thread pool and let it take care of parallelizing it and feeding
you results

It memory-maps the total range of bytes you're going to read, and then feeds your code
multiple `InputStream` instances each of which wraps an NIO `MappedByteBuffer` with the bytes from that
region, and which can be accessed concurrently.  So legacy code is happy, it looks like 
old-fashioned I/O code, but most of the time you'll be reading from the disk cache.

It attempts to guarantee the following:

 * The file will only be opened once (modulo anything you do ahead of time to figure out the region offsets)
 * Processing of regions happens concurrently, with blocking only for claiming the next region
 * You get notified when all regions have been processed

See the unit tests for a working example.

Basically:

```java

class MyInputStreamProcessor implements InputStreamProcessor<ParsedType> {

    public ParsedType process (RegionInputStream in) throws IOException {
        Region region = in.region(); // in case you're interested in
                                     // where in the file you are

        return // parse the region here and return an object representing what you read
    }
}

class MyRegionController implements RegionController<ParsedType> {
        @Override
        public void onOneDone(Region region, ParsedType result) {
            // do whatever you want with this result, or stuff it in a
            // concurrent collection and deal with them all in onDone()
        }

        @Override
        public void onException(Region region, Exception exception, InputStreamProcessor<ParsedType> processorThatFailed) {
            // Called if MyInputStreamProcessor.process() throws an exception
        }

        @Override
        public void onAllDone() {
            // Called ONLY when all chunks have been processed 
            // and onOneDone() has been called for each result
        }
}
```

Then you make a list of `Regions` - which is just a start and end byte offset, e.g.

```java
List<Region> regions = new ArrayList<>();
regions.add(new Region(200, 300));
// ...etc
```

and create however many processors you want - how many determines how many concurrent threads will
process the file (the `ExecutorService` thread pool you pass in should be able to spawn at least
that many threads).

```java
List<MyInputStreamProcessor> processors = new ArrayList<>();
processors.add(new MyInputStreamProcessor());
processors.add(new MyInputStreamProcessor());
processors.add(new MyInputStreamProcessor());
```

and pass it to `SplitFileProcessor.process()`, which takes care of the gory concurrency bookkeeping details.

```java
SplitFileProcessor.process(file, regions, new MyRegionController(), 
    threadPool, processors);
```

If the multiple calls to `processors.add()` seem rather dull to write, you can implement
`InputStreamProcessorFactory` and pass that in place of the list.

Parallelism
-----------

Processors run concurrently;  the number of concurrent threads doing processing is limited by
the number of processors and the size of the thread pool, whichever is smaller.  If the processor
code does its own blocking I/O, it may make sense to use more threads than you have cores, since
those threads will spend much of their time sleeping waiting for the OS to complete some I/O.

Caveats
-------

Does not currently support the total number of bytes being larger than
 (i.e. `range[last].end - range[0].start` must be less than
`Integer.MAX_VALUE`).  It's easy to support larger files with a small code change,
but you lose the performance advantage of setting up the mapping only once, since
you'd need to map individual regions (or write a mini-memory manager to manage
multiple buffers that map multiple regions - doable but not fun).


Dependencies
------------

This project has one dependency, `com.mastfrog.util` which can be found in [this maven repo
here](https://timboudreau.com/builds).  The code used is small, and the library is MIT license,
so you are free to borrow it if having a dependency disturbs you.


License
-------

MIT license, do what thou wilt.
