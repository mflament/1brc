package org.yah.test;

import java.nio.charset.StandardCharsets;

public class Murmur3 implements HashFactory {
    private final int seed;

    public Murmur3(int seed) {
        this.seed = seed;
    }

    @Override
    public int hash(byte[] buffer, int offset, int length) {
        int h = seed, k, keyIdx = offset;
        /* Read in groups of 4. */
        for (int i = length >> 2; i > 0; i--) {
            // Here is a source of differing results across endiannesses.
            // A swap here has no effects on hash properties though.
            k = endian32(buffer, keyIdx);
            keyIdx += 4;
            h ^= murmur_32_scramble(k);
            h = h << 13 | h >>> 19;
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
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    private static int murmur_32_scramble(int k) {
        k *= 0xcc9e2d51;
        k = k << 15 | k >>> 17;
        k *= 0x1b873593;
        return k;
    }

    private static int endian32(byte[] key, int idx) {
        int i = key[idx];
        i |= key[idx + 1] << 8;
        i |= key[idx + 2] << 16;
        i |= key[idx + 3] << 24;
        return i;
    }

    public static void main(String[] args) {
        Murmur3 murmur3 = new Murmur3(0);
        byte[] bytes = "Gaillard".getBytes(StandardCharsets.UTF_8);
        int hash = murmur3.hash(bytes, 0, bytes.length);
        System.out.println(Integer.toUnsignedLong(hash) + " , " + (hash == (int) 2611759141L));
    }

}
