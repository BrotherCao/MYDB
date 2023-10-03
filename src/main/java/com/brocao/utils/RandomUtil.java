package com.brocao.utils;

import java.security.SecureRandom;

public class RandomUtil {

    public static byte[] randomBytes(int length) {
        SecureRandom secureRandom = new SecureRandom();
        byte[] bytes = new byte[length];
        secureRandom.nextBytes(bytes);
        return bytes;
    }
}
