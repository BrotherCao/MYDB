package com.brocao.utils;

public class Types {
    public static long addressToUid(int pgNo,short offset) {
        long u0 = (long) pgNo;
        long u1 = (long) offset;
        return u0 << 32 | u1;//
    }
}
