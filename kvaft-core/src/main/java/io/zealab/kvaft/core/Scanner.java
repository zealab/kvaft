package io.zealab.kvaft.core;

/**
 * @author LeonWong
 */
public interface Scanner {

    /**
     * trigger when class was scanned
     *
     * @param clazz class
     */
    void onClazzScanned(Class<?> clazz);

    /**
     * scan package uri
     *
     * @return uri
     */
    String scanPackage();
}
