package io.zealab.kvaft.rpc.protoc;

import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import io.zealab.kvaft.core.Scanner;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

/**
 * @author LeonWong
 */
@Slf4j
public class ProtocHandleManager implements Scanner {

    /**
     * class to proto handle
     */
    private final static Map<String, MethodHandle> PROTOC_HANDLES = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io.zealab.kvaft";

    private ProtocHandleManager() {

    }

    public static ProtocHandleManager getInstance() {
        return SingletonHolder.instance;
    }


    private void handleMessageClass(Class<?> clazz) throws NoSuchMethodException, IllegalAccessException {
        boolean isMatch = Message.class.isAssignableFrom(clazz);
        if (isMatch) {
            MethodType mt = MethodType.methodType(clazz, byte[].class);
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            MethodHandle handle = lookup.findStatic(clazz, "parseFrom", mt);
            PROTOC_HANDLES.put(clazz.getName(), handle);
        }
    }

    public MethodHandle getHandle(String className) {
        return PROTOC_HANDLES.get(className);
    }

    @Override
    public void onClazzScanned(Class<?> clazz) {
        if (clazz.getName().startsWith(PACKAGE_SCAN)) {
            try {
                handleMessageClass(clazz);
                Class<?>[] innerClasses = clazz.getDeclaredClasses();
                for (Class<?> innerClazz : innerClasses) {
                    handleMessageClass(innerClazz);
                }
            } catch (Exception ex) {
                log.error("handle message class failed");
            }
        }
    }

    @Override
    public String scanPackage() {
        return PACKAGE_SCAN;
    }

    private static class SingletonHolder {
        public static ProtocHandleManager instance = new ProtocHandleManager();
    }
}
