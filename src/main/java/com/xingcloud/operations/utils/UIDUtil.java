package com.xingcloud.operations.utils;

/**
 * Created by wanghaixing on 15-2-9.
 */
public class UIDUtil {
    public static long transformerUID(byte[] hashUID){
        int offset = 5;
        byte[] newBytes = new byte[offset];
        System.arraycopy(hashUID, 0, newBytes, 0, offset);
        long samplingUid = 0;
        for (int i = 0; i < offset; i++) {
            samplingUid <<= 8;
            samplingUid ^= newBytes[i] & 0xFF;
        }
        return samplingUid;
    }

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static long toLong(byte[] bytes) throws Exception {
        return toLong(bytes, 0, 8);
    }

    public static long toLong(byte[] bytes, int offset, final int length) throws Exception {
        if (length != 8 || offset + length > bytes.length) {
            throw new Exception(
                    "to long exception " + "offset " + offset + " length " + length + " bytes.len " + bytes.length);
        }
        long l = 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    public static Long truncate(long hashedUID) throws Exception {
        byte[] bytes, newBytes;
        bytes = toBytes(hashedUID);
        newBytes = new byte[bytes.length];
        System.arraycopy(bytes, 4, newBytes, 4, 4);
        return toLong(newBytes);
    }
}
