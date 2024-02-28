package org.yah.test;

import java.nio.charset.StandardCharsets;

import static java.lang.Integer.rotateLeft;

public final class XXHash32 implements HashFactory {

    private static final int BUF_SIZE = 16;
    private static final int ROTATE_BITS = 13;

    private static final int PRIME1 = (int) 2654435761L;
    private static final int PRIME2 = (int) 2246822519L;
    private static final int PRIME3 = (int) 3266489917L;
    private static final int PRIME4 = 668265263;
    private static final int PRIME5 = 374761393;

    /**
     * Gets the little-endian int from 4 bytes starting at the specified index.
     *
     * @param buffer The data
     * @param idx    The index
     * @return The little-endian int
     */
    private static int getInt(final byte[] buffer, final int idx) {
        return buffer[idx] & 0xff |
                (buffer[idx + 1] & 0xff) << 8 |
                (buffer[idx + 2] & 0xff) << 16 |
                (buffer[idx + 3] & 0xff) << 24;
    }

    private final byte[] oneByte = new byte[1];
    private final int[] state = new int[4];
    // Note: the code used to use ByteBuffer but the manual method is 50% faster
    // See: https://gitbox.apache.org/repos/asf/commons-compress/diff/2f56fb5c
    private final byte[] buffer = new byte[BUF_SIZE];

    private final int seed;
    private int totalLen;

    private int pos;

    /**
     * Sets to true when the state array has been updated since the last reset.
     */
    private boolean stateUpdated;

    /**
     * Creates an XXHash32 instance with a seed of 0.
     */
    public XXHash32() {
        this(0);
    }

    /**
     * Creates an XXHash32 instance.
     *
     * @param seed the seed to use
     */
    public XXHash32(final int seed) {
        this.seed = seed;
        initializeState();
    }

    private void initializeState() {
        state[0] = seed + PRIME1 + PRIME2;
        state[1] = seed + PRIME2;
        state[2] = seed;
        state[3] = seed - PRIME1;
    }

    private void process(final byte[] b, final int offset) {
        // local shadows for performance
        int s0 = state[0];
        int s1 = state[1];
        int s2 = state[2];
        int s3 = state[3];

        s0 = rotateLeft(s0 + getInt(b, offset) * PRIME2, ROTATE_BITS) * PRIME1;
        s1 = rotateLeft(s1 + getInt(b, offset + 4) * PRIME2, ROTATE_BITS) * PRIME1;
        s2 = rotateLeft(s2 + getInt(b, offset + 8) * PRIME2, ROTATE_BITS) * PRIME1;
        s3 = rotateLeft(s3 + getInt(b, offset + 12) * PRIME2, ROTATE_BITS) * PRIME1;

        state[0] = s0;
        state[1] = s1;
        state[2] = s2;
        state[3] = s3;

        stateUpdated = true;
    }

    public void reset() {
        initializeState();
        totalLen = 0;
        pos = 0;
        stateUpdated = false;
    }

    public int getValue() {
        int hash;
        if (stateUpdated) {
            // Hash with the state
            hash = rotateLeft(state[0], 1) + rotateLeft(state[1], 7) + rotateLeft(state[2], 12) + rotateLeft(state[3], 18);
        }
        else {
            // Hash using the original seed from position 2
            hash = state[2] + PRIME5;
        }
        hash += totalLen;

        int idx = 0;
        final int limit = pos - 4;
        for (; idx <= limit; idx += 4) {
            hash = rotateLeft(hash + getInt(buffer, idx) * PRIME3, 17) * PRIME4;
        }
        while (idx < pos) {
            hash = rotateLeft(hash + (buffer[idx++] & 0xff) * PRIME5, 11) * PRIME1;
        }

        hash ^= hash >>> 15;
        hash *= PRIME2;
        hash ^= hash >>> 13;
        hash *= PRIME3;
        hash ^= hash >>> 16;
        return hash;
    }

    private void update(final byte[] b, int off, final int len) {
        if (len <= 0) {
            return;
        }
        totalLen += len;

        final int end = off + len;

        // Check if the unprocessed bytes and new bytes can fill a block of 16.
        // Make this overflow safe in the event that len is Integer.MAX_VALUE.
        // Equivalent to: (pos + len < BUF_SIZE)
        if (pos + len - BUF_SIZE < 0) {
            System.arraycopy(b, off, buffer, pos, len);
            pos += len;
            return;
        }

        // Process left-over bytes with new bytes
        if (pos > 0) {
            final int size = BUF_SIZE - pos;
            System.arraycopy(b, off, buffer, pos, size);
            process(buffer, 0);
            off += size;
        }

        final int limit = end - BUF_SIZE;
        while (off <= limit) {
            process(b, off);
            off += BUF_SIZE;
        }

        // Handle left-over bytes
        if (off < end) {
            pos = end - off;
            System.arraycopy(b, off, buffer, 0, pos);
        }
        else {
            pos = 0;
        }
    }

    @Override
    public int hash(byte[] s, int offset, int length) {
        reset();
        update(s, offset, length);
        return getValue();
    }

    public static void main(String[] args) {
        XXHash32 xxh32 = new XXHash32(0);
        byte[] bytes = "Gaillard".getBytes(StandardCharsets.UTF_8);
        int hash = xxh32.hash(bytes, 0, bytes.length);
        System.out.println(Integer.toUnsignedLong(hash) + " , " + (hash == (int) 2959073022L));
    }
}
