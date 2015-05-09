package com.timboudreau.niothing;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * An InputStream that can tell you the region of a file it
 * represents.
 *
 * @author Tim Boudreau
 */
public abstract class RegionInputStream extends FilterInputStream {
    private final Region region;

    RegionInputStream(Region region, InputStream in) {
        super(in);
        this.region = region;
    }

    public final Region region() {
        return region;
    }
}
