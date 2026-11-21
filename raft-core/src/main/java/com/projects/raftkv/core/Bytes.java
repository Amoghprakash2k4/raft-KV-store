package com.projects.raftkv.core;

import java.util.Arrays;
import java.util.HexFormat;

/**
 * Immutable byte string with value equality. Records holding a raw byte[] compare by
 * reference, which would silently break Log Matching checks in the simulator.
 */
public final class Bytes {
    public static final Bytes EMPTY = new Bytes(new byte[0]);
    private final byte[] data;

    private Bytes(byte[] data) { this.data = data; }

    public static Bytes copyOf(byte[] src) { return new Bytes(src.clone()); }
    public byte[] toByteArray() { return data.clone(); }
    public int size() { return data.length; }

    @Override public boolean equals(Object o) {
        return o instanceof Bytes b && Arrays.equals(data, b.data);
    }
    @Override public int hashCode() { return Arrays.hashCode(data); }
    @Override public String toString() {
        int n = Math.min(data.length, 16);
        return "Bytes[" + data.length + "]{" + HexFormat.of().formatHex(data, 0, n)
                + (data.length > n ? "..." : "") + "}";
    }
}
