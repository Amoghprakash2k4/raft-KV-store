package com.projects.raftkv.storage;

/** A durability failure. Unrecoverable for the running process: the node must halt and recover. */
public final class StorageException extends RuntimeException {
    public StorageException(String message) { super(message); }
    public StorageException(String message, Throwable cause) { super(message, cause); }
}
