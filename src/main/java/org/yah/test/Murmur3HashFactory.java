package org.yah.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Murmur3HashFactory implements HashFactory {
    private final int seed;

    public Murmur3HashFactory(int seed) {
        this.seed = seed;
    }

    @Override
    public int hash(byte[] buffer, int offset, int length) {
        int h = seed, k, keyIdx = offset;
        /* Read in groups of 4. */
        for (int i = length >> 2; i > 0; i--) {
            // Here is a source of differing results across endiannesses.
            // A swap here has no effects on hash properties though.
            k = toInt(buffer, keyIdx);
            keyIdx += 4;
            h ^= murmur_32_scramble(k);
            h = (h << 13) | (h >> 19);
            h = h * 5 + 0xe6546b64;
        }
        /* Read the rest. */
        k = 0;
        for (int i = length & 3; i > 0; i--) {
            k <<= 8;
            k |= buffer[keyIdx + i - 1];
        }
        // A swap is *not* necessary here because the preceding loop already
        // places the low bytes in the low places according to whatever endianness
        // we use. Swaps only apply when the memory is copied in a chunk.
        h ^= murmur_32_scramble(k);
        /* Finalize. */
        h ^= length;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

    private static int murmur_32_scramble(int k) {
        k *= 0xcc9e2d51;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593;
        return k;
    }

    private static int toInt(byte[] key, int idx) {
        return ByteBuffer.wrap(key, idx, 4).order(ByteOrder.LITTLE_ENDIAN).getInt(0);
//        int i = key[idx];
//        i |= key[idx + 1] << 8;
//        i |= key[idx + 2] << 16;
//        i |= key[idx + 3] << 24;

//        int i = key[idx + 3];
//        i |= key[idx + 2] << 8;
//        i |= key[idx + 1] << 16;
//        i |= key[idx] << 24;
//        return i;
    }

}
