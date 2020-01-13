package io.zealab.kvaft.rpc.protoc;

import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

/**
 * @author LeonWong
 */
@Slf4j
public class ProtocHandleManager implements Initializer {

    /**
     * class to proto handle
     */
    private final static Map<String, MethodHandle> PROTOC_HANDLES = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io/zealab/kvaft";

    private final static ClassLoader cl = ChannelProcessorManager.class.getClassLoader();

    public ProtocHandleManager() {
        init();
    }

    @Override
    public void init() {
        String packageName = PACKAGE_SCAN.replaceAll("/", "\\.");
        URL url = cl.getResource(PACKAGE_SCAN);
        if (Objects.nonNull(url)) {
            try {
                URI uri = url.toURI();
                File file = new File(uri);
                file.listFiles(
                        childFile -> {
                            recursiveLoad(childFile, packageName);
                            return true;
                        }
                );
            } catch (URISyntaxException e) {
                log.error("The uri from protoc handle scan package doesn't exist", e);
            }
        }
    }

    private void recursiveLoad(File childFile, String packageName) {
        if (childFile.isFile()) {
            if (childFile.getName().endsWith(".class")) {
                String clazzName = String.format("%s.%s", packageName, childFile.getName());
                clazzName = clazzName.substring(0, clazzName.indexOf(".class"));
                try {
                    Class<?> clazz = Class.forName(clazzName);
                    handleMessageClass(clazz);
                    Class<?>[] inners = clazz.getDeclaredClasses();
                    ;
                    for (Class<?> inner : inners) {
                        handleMessageClass(inner);
                    }
                } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException e) {
                    log.error("recursive proto handle failed", e);
                }
            }
        } else if (childFile.isDirectory()) {
            StringBuilder packageBuilder = new StringBuilder(packageName);
            packageBuilder.append(".").append(childFile.getName());
            String dir = packageBuilder.toString().replaceAll("\\.", "/");
            URL url = cl.getResource(dir);
            if (Objects.nonNull(url)) {
                try {
                    URI uri = url.toURI();
                    File file = new File(uri);
                    file.listFiles(
                            innerFile -> {
                                recursiveLoad(innerFile, packageBuilder.toString());
                                return true;
                            }
                    );
                } catch (URISyntaxException e) {
                    log.error("The scan package of recursive proto handles doesn't exist", e);
                }
            }
        }
    }

    private void handleMessageClass(Class<?> clazz) throws NoSuchMethodException, IllegalAccessException {
        Class<?>[] interfaces = clazz.getInterfaces();
        boolean isMatch = false;
        for (Class<?> inter : interfaces) {
            if (inter.equals(Message.class)) {
                isMatch = true;
                break;
            }
        }
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
}
