package com.projects.raftkv.storage.disk;

import java.util.List;

/**
 * One flat directory of files: the only file-system operations the WAL and snapshot store use.
 * <p>
 * Durability contract, modelled on POSIX:
 * <ul>
 *   <li>{@link #append} and {@link #truncate} change the file's contents in the page cache; they are
 *       durable only after {@link #fsync} on that file.</li>
 *   <li>Creating, renaming or deleting a file changes the directory; that is durable only after
 *       {@link #fsyncDir}. fsync on a file does not make its name durable.</li>
 *   <li>{@link #rename} atomically replaces the target, if any.</li>
 * </ul>
 * Every method throws {@link com.projects.raftkv.storage.StorageException} on an I/O error. A failed
 * fsync leaves the durable state unknown, so the caller must halt (DESIGN.md E1a, fail-stop).
 */
public interface Disk extends AutoCloseable {

    /** Names of all files, sorted. */
    List<String> list();

    boolean exists(String name);

    long size(String name);

    /** The whole file. */
    byte[] read(String name);

    /** Appends to the file, creating it if it does not exist. */
    void append(String name, byte[] data);

    /** Shrinks the file to {@code size} bytes. */
    void truncate(String name, long size);

    /** Makes the file's contents durable. */
    void fsync(String name);

    /** Atomically renames {@code from} to {@code to}, replacing {@code to} if it exists. */
    void rename(String from, String to);

    void delete(String name);

    /** Makes all creates, renames and deletes so far durable. */
    void fsyncDir();

    /** Releases open handles. Does not sync. */
    @Override void close();
}
