package org.yah.test;

import java.nio.charset.StandardCharsets;

public class XXH32 implements HashFactory {

    private static final int PRIME1 = 0x9E3779B1;
    private static final int PRIME2 = 0x85EBCA77;
    private static final int PRIME3 = 0xC2B2AE3D;
    private static final int PRIME4 = 0x27D4EB2F;
    private static final int PRIME5 = 0x165667B1;

    private final int seed;

    public XXH32(int seed) {
        this.seed = seed;
    }

    @Override
    public int hash(byte[] input, int offset, int len) {
        return finalize((len >= 16 ? h16bytes(input, offset, len, seed) : seed + PRIME5) + len, input, offset + (len & ~0xF), len & 0xF);
    }

    // 32-bit rotate left.
    private static int rotl(int x, int r) {
        return ((x << r) | (x >>> (32 - r)));
    }

    // Normal stripe processing routine.
    private static int round(int acc, int input) {
        return rotl(acc + (input * PRIME2), 13) * PRIME1;
    }

    private static int avalanche_step(int h, int rshift, int prime) {
        return (h ^ (h >>> rshift)) * prime;
    }

    // Mixes all bits to finalize the hash.
    private static int avalanche(int h) {
        return avalanche_step(avalanche_step(avalanche_step(h, 15, PRIME2), 13, PRIME3), 16, 1);
    }

    private static int fetch32(byte[] p, int offset, int v) {
        return round(v, endian32(p, offset));
    }

    // Processes the last 0-15 bytes of p.
    private static int finalize(int h, byte[] p, int offset, int len) {
        return (len >= 4)
                ? finalize(rotl(h + (endian32(p, offset) * PRIME3), 17) * PRIME4, p, offset + 4, len - 4)
                : (len > 0) ? finalize(rotl(h + (p[offset] * PRIME5), 11) * PRIME1, p, offset + 1, len - 1) : avalanche(h);
    }

    private static int h16bytes(byte[] p, int offset, int len, int v1, int v2, int v3, int v4) {
        return (len >= 16)
                ? h16bytes(p, offset + 16, len - 16, fetch32(p, offset, v1), fetch32(p, offset + 4, v2), fetch32(p, offset + 8, v3), fetch32(p, offset + 12, v4))
                : rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
    }

    private static int h16bytes(byte[] p, int offset, int len, int seed) {
        return h16bytes(p, offset, len, seed + PRIME1 + PRIME2, seed + PRIME2, seed, seed - PRIME1);
    }

    private static int endian32(byte[] key, int idx) {
        int i = key[idx];
        i |= key[idx + 1] << 8;
        i |= key[idx + 2] << 16;
        i |= key[idx + 3] << 24;
        return i;
    }

    public static void main(String[] args) {
        XXH32 xxh32 = new XXH32(0);
        byte[] bytes = "Gaillard".getBytes(StandardCharsets.UTF_8);
        int hash = xxh32.hash(bytes, 0, bytes.length);
        System.out.println(Integer.toUnsignedLong(hash) + " , " + (hash == (int) 2959073022L));
    }
}
