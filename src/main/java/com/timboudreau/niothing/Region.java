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
