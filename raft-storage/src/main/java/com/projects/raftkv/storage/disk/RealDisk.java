package com.projects.raftkv.storage.disk;

import com.projects.raftkv.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * {@link Disk} on a real directory. fsync is {@code FileChannel.force(true)}; the directory is synced
 * by forcing a channel opened on the directory itself (works on Linux).
 * <p>
 * On macOS, force() calls fsync(2), which does not flush the drive's own write cache (that needs
 * fcntl F_FULLFSYNC), and opening a directory as a channel may fail, in which case fsyncDir is a no-op.
 * See DEVIATIONS.md. Linux, as in the Docker cluster, is the real target.
 */
public final class RealDisk implements Disk {

    private final Path dir;
    private final Map<String, FileChannel> open = new HashMap<>();

    public RealDisk(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StorageException("cannot create " + dir, e);
        }
    }

    @Override public List<String> list() {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new StorageException("cannot list " + dir, e);
        }
    }

    @Override public boolean exists(String name) { return Files.isRegularFile(path(name)); }

    @Override public long size(String name) {
        try {
            return Files.size(path(name));
        } catch (IOException e) {
            throw new StorageException("cannot stat " + name, e);
        }
    }

    @Override public byte[] read(String name) {
        try {
            return Files.readAllBytes(path(name));
        } catch (NoSuchFileException e) {
            throw new StorageException("no such file: " + name, e);
        } catch (IOException e) {
            throw new StorageException("cannot read " + name, e);
        }
    }

    @Override public void append(String name, byte[] data) {
        try {
            FileChannel ch = channel(name);
            ByteBuffer buf = ByteBuffer.wrap(data);
            long pos = ch.size();
            while (buf.hasRemaining()) pos += ch.write(buf, pos);
        } catch (IOException e) {
            throw new StorageException("cannot append to " + name, e);
        }
    }

    @Override public void truncate(String name, long size) {
        try {
            channel(name).truncate(size);
        } catch (IOException e) {
            throw new StorageException("cannot truncate " + name, e);
        }
    }

    @Override public void fsync(String name) {
        try {
            channel(name).force(true);
        } catch (IOException e) {
            throw new StorageException("fsync failed for " + name, e);
        }
    }

    @Override public void rename(String from, String to) {
        closeChannel(from);
        closeChannel(to);
        try {
            Files.move(path(from), path(to), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("cannot rename " + from + " to " + to, e);
        }
    }

    @Override public void delete(String name) {
        closeChannel(name);
        try {
            Files.deleteIfExists(path(name));
        } catch (IOException e) {
            throw new StorageException("cannot delete " + name, e);
        }
    }

    @Override public void fsyncDir() {
        FileChannel ch;
        try {
            ch = FileChannel.open(dir, StandardOpenOption.READ);
        } catch (IOException e) {
            return;   // the platform cannot open directories (see class comment)
        }
        try (ch) {
            ch.force(true);
        } catch (IOException e) {
            throw new StorageException("fsync failed for directory " + dir, e);
        }
    }

    @Override public void close() {
        for (String name : List.copyOf(open.keySet())) closeChannel(name);
    }

    private FileChannel channel(String name) throws IOException {
        FileChannel ch = open.get(name);
        if (ch == null) {
            ch = FileChannel.open(path(name), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            open.put(name, ch);
        }
        return ch;
    }

    private void closeChannel(String name) {
        FileChannel ch = open.remove(name);
        if (ch == null) return;
        try {
            ch.close();
        } catch (IOException e) {
            throw new StorageException("cannot close " + name, e);
        }
    }

    private Path path(String name) {
        if (name.isEmpty() || name.contains("/") || name.contains("\\") || name.startsWith("."))
            throw new IllegalArgumentException("bad file name: " + name);
        return dir.resolve(name);
    }
}
