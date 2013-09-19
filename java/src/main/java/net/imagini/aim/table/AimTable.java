package net.imagini.aim.table;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.imagini.aim.Aim;
import net.imagini.aim.Aim.SortOrder;
import net.imagini.aim.AimFilter;
import net.imagini.aim.AimSchema;
import net.imagini.aim.AimSegment;
import net.imagini.aim.AimType;
import net.imagini.aim.AimTypeAbstract.AimDataType;
import net.imagini.aim.AimUtils;
import net.imagini.aim.ByteKey;
import net.imagini.aim.Pipe;

/**
 * @author mharis
 */
public class AimTable {

    public final AimSchema schema;
    public final String name;
    public final Integer segmentSizeBytes;
    public final Integer sortColumn;
    public final SortOrder sortOrder;
    private LinkedList<AimSegment> segments = new LinkedList<>();
    private AtomicInteger numSegments = new AtomicInteger(0);
    private AtomicLong originalSize = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong size = new AtomicLong(0);

    //TODO configure executor per table server
    final ExecutorService executor = Executors.newFixedThreadPool(4);

    public AimTable(String name, Integer segmentSizeBytes, AimSchema schema, String sortField, SortOrder order) throws IOException {
        this.name = name;
        this.sortColumn = schema.get(sortField);
        this.sortOrder = order;
        this.schema = schema;
        this.segmentSizeBytes = segmentSizeBytes;
        System.out.println("Table " + name + " (" + schema.toString() + ")");
    }

    public int getSortColumn() {
        return sortColumn;
    }

    public void add(AimSegment segment) {

        synchronized(segments) {
            segments.add(segment);
        }

        numSegments.incrementAndGet();
        count.addAndGet(segment.getCount());
        size.addAndGet(segment.getSize());
        originalSize.addAndGet(segment.getOriginalSize());
    }

    public AimType def(String colName) {
        checkColumn(colName);
        return schema.def(colName);
    }

    public int getNumSegments() {
        return numSegments.get();
    }

    public long getCount() {
        return count.get();
    }
    public long getSize() {
        return size.get();
    }

    public long getOriginalSize() {
        return originalSize.get();
    }

    private void checkColumn(String colName) throws IllegalArgumentException {
        if (!schema.has(colName)) {
            throw new IllegalArgumentException(
                "Column `"+colName+"` is not defined for table `"+name+"`. Available columns are: " 
                + schema.describe()
            );
        }
    }

    /**
     * Open single segment.
     * @param segmentId
     * @return
     * @throws IOException
     */
    public AimSegment open(int segmentId) throws IOException {
        synchronized(segments) {
            return segmentId >=0 && segmentId < segments.size() ? segments.get(segmentId) : null;
        }
    }

    /**
     * Parallel count - currently hard-coded for 4-core processors, however once the table
     * is distributed across multiple machines this thread pool should be bigger until the 
     * I/O becomes bottleneck.
     */
    public long count(
        int startSegment, 
        int endSegment, 
        final AimFilter filter
    ) throws IOException {
        final int[] seg = new int[endSegment-startSegment+1];
        for(int i=startSegment; i<= endSegment; i++) seg[i-startSegment] = i;
        final List<Future<Long>> results = new ArrayList<Future<Long>>();
        for(final int s: seg) {
            results.add(executor.submit(new Callable<Long>() {
                @Override public Long call() throws Exception {
                    AimSegment segment;
                    synchronized(segments) {
                        segment = segments.get(s);
                    }
                    return segment.count(filter);
                }
            }));
        }
        long count = 0;
        Exception error = null; 
        for(Future<Long> result: results) {
            try {
                count += result.get();
            } catch (ExecutionException e) {
                error = e ;
                break;
            } catch (InterruptedException e) {
                error = e;
                break;
            }
        }
        if (error != null) {
            throw new IOException(error);
        }
        return count;
    }

    /**
     * ZeroCopy buffered multi-threaded implementation of select stream. 
     * Open multiple segments - this is where streaming-merge-sort happens
     * Note: merge sort is combined with TreeMap O=log(n) indexing so
     * that for large number of segments it still performs well.
     */
    public Pipe select(
        int startSegment, 
        int endSegment,
        final AimFilter filter, 
        final String... columnNames
    ) throws IOException {
        final int numSegments = endSegment-startSegment+1;
        final AimSchema subSchema = schema.subset(columnNames);
        final int sortSubColumn = subSchema.get(schema.name(sortColumn));

        final int[] seg = new int[numSegments];
        for(int i=startSegment; i<= endSegment; i++) seg[i-startSegment] = i;

        final Fetcher[] fetchers = new Fetcher[numSegments];
        Future<?>[] x = new Future<?>[numSegments];
        for(int s = 0; s <seg.length; s++) {
            fetchers[s] = new Fetcher(
                segments.get(seg[s]).select(filter, schema.name(sortColumn), columnNames),
                subSchema
            );
            x[s] = executor.submit(fetchers[s]);
        }
        //FIXME this is just a temporary hack to get the buffers rolling before reading from the pipe:
        for(Future<?> f: x) try { f.get(); } catch (Exception e) { }

        return new Pipe() {
            private int currentSegment = -1;
            private int currentColumn = columnNames.length-1;
            private TreeMap<ByteKey,Integer> sortIndex = new TreeMap<>();
            final private Boolean[] hasData = new Boolean[numSegments];

            @Override
            public void skip(AimDataType type) throws IOException {
                if (++currentColumn == columnNames.length) {
                    currentColumn = 0;
                    readNextRecord();
                }
            }
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++currentColumn == columnNames.length) {
                    currentColumn = 0;
                    readNextRecord();
                }
                int p = (int)(fetchers[currentSegment].position % rollingBufferSize);
                return fetchers[currentSegment].buffer[currentColumn][p];
            }
            private void readNextRecord() throws IOException {
                if (currentSegment ==-1 ) {
                    for(int s = 0; s <seg.length; s++) {
                        sortNextRecordFromSegment(s);
                    }
                } else if (hasData[currentSegment]) {
                    sortNextRecordFromSegment(currentSegment);
                }
                if (sortIndex.size() == 0) {
                    throw new EOFException();
                }
                Entry<ByteKey,Integer> next;
                switch(sortOrder) {
                    case DESC: next = sortIndex.pollLastEntry();break;
                    default: case ASC: next = sortIndex.pollFirstEntry();break;
                }
                currentSegment = next.getValue();
            }
            private void sortNextRecordFromSegment(int s) throws IOException {
                if (!fetchers[s].next()) {
                    hasData[s] = false;
                    return;
                }
                int p = (int)(fetchers[s].position % rollingBufferSize);
                sortIndex.put(new ByteKey(fetchers[s].buffer[sortSubColumn][p],s), s);
                hasData[s] = true;
            }
        };
    }

    private static int rollingBufferSize = 8;

    private class Fetcher implements Runnable {

        private long position = -1;
        private long limit = 0;
        public boolean eof = false;
        final private Object lock = new Object();
        final public byte[][][] buffer;
        final private AimSchema schema;
        final private InputStream stream;
        public Fetcher(InputStream stream, AimSchema schema) {
            this.schema = schema;
            this.stream = stream;
            buffer = new byte[schema.size()][rollingBufferSize][Aim.COLUMN_BUFFER_SIZE];
        }
        public boolean next() {
            position++;
            while (position == limit) {
                if (position == limit) {
                    if (eof) {
                        return false;
                    }
                    synchronized(lock) {
                        try { lock.wait(); } catch (InterruptedException e) {}
                    }
                }
            }
            if (!eof && position + 1 == limit) {
                synchronized(lock) {
                    if (!eof && position + 1 == limit) {
                        AimTable.this.executor.execute(this);
                    }
                }
            }
            return true;
        }
        @Override public void run() {
            synchronized(lock) {
                try {
                    while (limit - (Math.max(0,position)) < rollingBufferSize) {
                        int l = (int)(limit % rollingBufferSize);
                        for(String colName: schema.names()) {
                            int c = schema.get(colName);
                            AimType type = schema.get(c);
                            AimUtils.read(stream,type.getDataType(), buffer[c][l]);
                        }
                        limit++;
                    }
                    lock.notify();
                } catch (EOFException e) {
                    eof = true;
                    lock.notify();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * ZeroCopy single-threaded implementation of select stream. 
     * Open multiple segments - this is where streaming-merge-sort happens
     * Note: merge sort is combined with TreeMap O=log(n) indexing so
     * that for large number of segments it still performs well.
     *
    public Pipe select(
        int startSegment, 
        int endSegment,
        final AimFilter filter, 
        final String... columnNames
    ) throws IOException {
        final int[] seg = new int[endSegment-startSegment+1];
        for(int i=startSegment; i<= endSegment; i++) seg[i-startSegment] = i;
        final InputStream[] str = new InputStream[segments.size()];
        final AimSchema subSchema = schema.subset(columnNames);
        for(int s = 0; s <seg.length; s++) {
            str[s] = segments.get(seg[s]).select(filter, schema.name(sortColumn), columnNames);
        }

        final int sortSubColumn = subSchema.get(schema.name(sortColumn));

        return new Pipe() {
            private int currentSegment = -1;
            private int currentColumn = columnNames.length-1;
            private TreeMap<ByteKey,Integer> sortIndex = new TreeMap<>();
            final private Boolean[] hasData = new Boolean[segments.size()];
            final private byte[][][] buffer = new byte[segments.size()][schema.size()][Aim.COLUMN_BUFFER_SIZE];
            @Override
            public void skip(AimDataType type) throws IOException {
                if (++currentColumn == columnNames.length) {
                    currentColumn = 0;
                    readNextRecord();
                }
            }
            @Override public byte[] read(AimDataType type) throws IOException {
                if (++currentColumn == columnNames.length) {
                    currentColumn = 0;
                    readNextRecord();
                }
                return buffer[currentSegment][currentColumn];
            }
            private void readNextRecord() throws IOException {
                if (currentSegment ==-1 ) {
                    for(int s = 0; s <seg.length; s++) {
                        loadRecordFromSegment(s);
                    }
                } else if (hasData[currentSegment]) {
                    loadRecordFromSegment(currentSegment);
                }
                if (sortIndex.size() == 0) {
                    throw new EOFException();
                }
                Entry<ByteKey,Integer> next;
                switch(sortOrder) {
                    case DESC: next = sortIndex.pollLastEntry();break;
                    default: case ASC: next = sortIndex.pollFirstEntry();break;
                }
                currentSegment = next.getValue();
            }
            private void loadRecordFromSegment(int s) throws IOException {
                try {
                    for(String colName: columnNames) {
                        int c = subSchema.get(colName);
                        AimType type = subSchema.get(c);
                        AimUtils.read(str[s],type.getDataType(), buffer[s][c]);
                    }
                    sortIndex.put(new ByteKey(buffer[s][sortSubColumn],s), s);
                    hasData[s] = true;
                } catch (EOFException e) {
                    hasData[s] = false;
                }
            }
        };
    }
    */
}
