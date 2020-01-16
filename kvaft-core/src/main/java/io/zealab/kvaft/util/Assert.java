package io.zealab.kvaft.util;

/**
 * @author LeonWong
 */
public class Assert {

    public static void state(boolean state, String message) {
        if (!state) {
            throw new IllegalStateException(message);
        }
    }

    public static void notNull(Object obj, String message) {
        if (obj == null) {
            throw new IllegalStateException(message);
        }
    }
}
