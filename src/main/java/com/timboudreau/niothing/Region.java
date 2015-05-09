package com.timboudreau.niothing;

/**
 * A pair of byte offsets into a file.
 *
 * @author Tim Boudreau
 */
public final class Region implements Comparable<Region> {

    public final long start;
    public final long end;

    public static Region EMPTY = new Region(0, 0);

    public Region(long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("Start " + start + " > " + end);
        }
        if (end - start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(end - start + " will not fit in a ByteBuffer");
        }
        this.start = start;
        this.end = end;
    }

    public int length() {
        return (int) (end - start);
    }

    @Override
    public int compareTo(Region o) {
        Long a = start;
        Long b = o.start;
        return a.compareTo(b);
    }

    public String toString() {
        return start + "-" + end;
    }
}
