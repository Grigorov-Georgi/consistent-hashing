package org.example;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentHashRing<T> {

    private final TreeMap<Long, T> ring = new TreeMap<>();
    private final Map<T, List<Long>> nodeToKeys = new HashMap<>();
    private final int replicas;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(int replicas) {
        if (replicas <= 0) {
            throw new IllegalArgumentException("replicas must be greater than 0");
        }

        this.replicas = replicas;
    }

    public ConsistentHashRing() {
        this(100);
    }

    public void addNode(T node) {
        Objects.requireNonNull(node, "node");
        lock.writeLock().lock();
        try {
            if (nodeToKeys.containsKey(node)) {
                return; // idempotent
            }

            List<Long> keys = new ArrayList<>(replicas);
            for (int i = 0; i < replicas; i++) {
                long h = unsignedHash(node.toString() + "#" + i);
                while (ring.containsKey(h)) {
                    h = unsignedHash(node.toString() + "#" + i + ":");
                }
                ring.put(h, node);
                keys.add(h);
            }
            nodeToKeys.put(node, keys);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeNode(T node) {
        Objects.requireNonNull(node, "node");
        lock.writeLock().lock();
        try {
            List<Long> keys = nodeToKeys.remove(node);
            if (keys == null) {
                return; // idempotent
            }

            for (Long h : keys) {
                ring.remove(h);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public T getNode(String key) {
        Objects.requireNonNull(key, "key");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return null;
            }

            long h = unsignedHash(key);
            Map.Entry<Long, T> entry = ring.ceilingEntry(h);
            if (entry == null) {
                // wrap around
                entry = ring.firstEntry();
            }

            return entry.getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<T> getDistinctNodes(String key, int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be greater than 0");
        }
        Objects.requireNonNull(key, "key");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return Collections.emptyList();
            }

            long h = unsignedHash(key);
            NavigableMap<Long, T> tail = ring.tailMap(h, true);
            Iterator<Map.Entry<Long, T>> it1 = tail.entrySet().iterator();
            Iterator<Map.Entry<Long, T>> it2 = ring.headMap(h, true).entrySet().iterator();

            LinkedHashSet<T> distinct = new LinkedHashSet<>();
            Iterator<Map.Entry<Long, T>> it = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it1.hasNext() || it2.hasNext();
                }

                @Override
                public Map.Entry<Long, T> next() {
                    if (it1.hasNext()) {
                        return it1.next();
                    }

                    if (it2.hasNext()) {
                        return it2.next();
                    }
                    throw new NoSuchElementException();
                }
            };

            while (it.hasNext() && distinct.size() < k) {
                distinct.add(it.next().getValue());
            }

            return new ArrayList<>(distinct);

        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return ring.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<T> physicalNodes() {
        lock.readLock().lock();
        try {
            return new LinkedHashSet<>(nodeToKeys.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    private static long unsignedHash(String s) {
        int h = murmur3_32(s);
        return h & 0xFFFF_FFFFL;
    }

    public static int murmur3_32(String data) {
        byte[] bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int len = bytes.length;
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = 0;

        int i = 0;
        while (i + 4 <= len) {
            int k1 = ((bytes[i] & 0xff)) |
                    ((bytes[i + 1] & 0xff) << 8) |
                    ((bytes[i + 2] & 0xff) << 16) |
                    ((bytes[i + 3] & 0xff) << 24);
            i += 4;

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int remaining = len & 3;
        if (remaining == 3) {
            k1 ^= (bytes[i + 2] & 0xff) << 16;
        }
        if (remaining >= 2) {
            k1 ^= (bytes[i + 1] & 0xff) << 8;
        }
        if (remaining >= 1) {
            k1 ^= (bytes[i] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }

        h1 ^= len;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }
}
