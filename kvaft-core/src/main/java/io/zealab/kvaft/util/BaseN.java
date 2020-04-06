package io.zealab.kvaft.util;

import lombok.NonNull;

import java.util.Arrays;

/**
 * Base N codec
 *
 * @author leonwong
 */
public class BaseN {

    /**
     * 进制字母表
     */
    private static final char[] ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+/".toCharArray();

    /**
     * 字母表反向索引
     */
    private static final byte[] INDEXES = new byte[128];

    /**
     * byte 的原始进制应当是 256 进制数
     */
    private static final int ORI = 256;

    static {
        Arrays.fill(INDEXES, (byte) 0xf);
        for (int i = 0; i < ALPHABET.length; i++) {
            INDEXES[ALPHABET[i]] = (byte) i;
        }
    }

    /**
     * 根据给定的 n 进制进行编码
     *
     * tips: 将 256 进制转化成 N 进制
     *
     * @param data 原始数据
     * @param n    目标转化进制
     * @return
     */
    public static String encode(byte[] data, int n) {
        if (data == null || data.length == 0) {
            return "";
        }
        if (n > ALPHABET.length) {
            throw new IllegalArgumentException(String.format("The `n` must less and equals than %s", ALPHABET.length));
        }
        byte[] encoded = new byte[data.length * 8];
        for (int tail = encoded.length - 1, i = 0; i < data.length; ) {
            encoded[tail--] = divMod(data, ORI, i, n);
            if (data[i] == 0) {
                i++;
            }
        }
        encoded = truncate(encoded);
        char[] result = new char[encoded.length];
        for (int i = 0; i < encoded.length; i++) {
            result[i] = ALPHABET[encoded[i]];
        }
        return new String(result);
    }

    /**
     * 根据给定的 n 进制进行解码
     *
     * tips:将给定的 n 进制数转化为 256 进制
     *
     * @param data  baseN 编码结果
     * @param n     baseN 的进制数
     * @return
     */
    public static byte[] decode(@NonNull String baseN, int n) {
        if (baseN.isEmpty()) {
            return new byte[0];
        }
        byte[] input = new byte[baseN.length()];
        for (int i = 0; i < input.length; i++) {
            char c = baseN.charAt(i);
            if (c > 128) {
                throw new IllegalArgumentException("The `c` is out of range , decode failed ");
            }
            byte value = INDEXES[c];
            if (value == -1) {
                throw new IllegalArgumentException("The `c` is out of range , decode failed ");
            }
            input[i] = value;
        }
        byte[] decoded = new byte[input.length];
        for (int tail = decoded.length - 1, i = 0; i < input.length; ) {
            decoded[tail--] = divMod(input, n, i, ORI);
            if (input[i] == 0) {
                i++;
            }
        }
        return truncate(decoded);
    }

    /**
     * 62 进制编码
     *
     * @param data
     * @return
     */
    public static String encode62(byte[] data) {
        return encode(data, 62);
    }

    /**
     * 62 进制解码
     *
     * @param data
     * @return
     */
    public static byte[] decode62(String data) {
        return decode(data, 62);
    }

    /**
     * 64 进制编码
     *
     * @param data
     * @return
     */
    public static String encode64(byte[] data) {
        return encode(data, 64);
    }

    /**
     * 64 进制解码
     *
     * @param data
     * @return
     */
    public static byte[] decode64(String data) {
        return decode(data, 64);
    }

    /**
     * 纯数字10进制编码
     *
     * @param data
     * @return
     */
    public static String encode10(byte[] data) {
        return encode(data, 10);
    }

    /**
     * 纯数字10进制解码
     *
     * @param data
     * @return
     */
    public static byte[] decode10(String data) {
        return decode(data, 10);
    }

    /**
     * 数字+字母 不区分大小写编码
     *
     * @param data
     * @return
     */
    public static String encode36(byte[] data) {
        return encode(data, 36);
    }

    /**
     * 数字+字母 不区分大小写解码
     *
     * @param data
     * @return
     */
    public static byte[] decode36(String data) {
        return decode(data, 36);
    }


    /**
     * 通用求商求余进制转换算法：
     *
     * x divMod y = (q,r)  q=x/y, r=x%y
     *
     * @param input   原始数值，指代这里的 x
     * @param oriBase 原始数值进制
     * @param ptr     指针
     * @param divisor 除数，目标进制，指代这里的 y
     *
     * @return 返回余数 r，商的结果 q 回存到 input 里
     */
    public static byte divMod(byte[] input, int oriBase, int ptr, int divisor) {
        int remainders = 0;
        for (int i = ptr; i < input.length; i++) {
            int digit = input[i] & 0xff;
            int temp = remainders * oriBase + digit;
            input[i] = (byte) (temp / divisor);
            remainders = temp % divisor;
        }
        return (byte) remainders;
    }

    /**
     * 截断高位的 0
     *
     * @param encoded
     * @return
     */
    private static byte[] truncate(byte[] encoded) {
        int zeros = 0;
        for (byte b : encoded) {
            if (b != 0) {
                break;
            }
            zeros++;
        }
        if (zeros > 0) {
            byte[] truncateEncoded = new byte[encoded.length - zeros];
            System.arraycopy(encoded, zeros, truncateEncoded, 0, truncateEncoded.length);
            encoded = truncateEncoded;
        }
        return encoded;
    }
}
