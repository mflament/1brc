package org.yah.test;

public interface HashFactory {

    int hash(byte[] s, int offset, int length);

    default int hash(byte[] s, int length) {
        return hash(s, 0, length);
    }

}
