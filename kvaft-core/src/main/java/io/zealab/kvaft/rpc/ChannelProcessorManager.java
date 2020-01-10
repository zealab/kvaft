package io.zealab.kvaft.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Initializer;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

/**
 * channel processors manager
 *
 * @author LeonWong
 */
@Slf4j
public class ChannelProcessorManager implements Initializer {

    private final static Map<String, ChannelProcessor<?>> REGISTRY = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io/zealab/kvaft";

    private final static ClassLoader cl = ChannelProcessorManager.class.getClassLoader();

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
                log.error("The uri from channel process scan package doesn't exist", e);
            }
        }
    }

    /**
     * load class name into map
     *
     * @param childFile   file
     * @param packageName package
     */
    private void recursiveLoad(File childFile, String packageName) {
        if (childFile.isFile()) {
            if (childFile.getName().endsWith(".class")) {
                String clazzName = String.format("%s.%s", packageName, childFile.getName());
                clazzName = clazzName.substring(0, clazzName.indexOf(".class"));
                try {
                    Class<?> clazz = Class.forName(clazzName);
                    Processor kvaftProcessor = clazz.getAnnotation(Processor.class);
                    if (Objects.nonNull(kvaftProcessor)) {
                        Object instance = clazz.newInstance();
                        if (instance instanceof ChannelProcessor) {
                            REGISTRY.putIfAbsent(kvaftProcessor.messageClazz().toString(), (ChannelProcessor<?>) instance);
                        }
                    }
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                    log.error("class reflect failed", e);
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
                    log.error("The uri from channel process scan package doesn't exist", e);
                }
            }
        }
    }

    /**
     * get payload processor
     *
     * @param payloadClazz clazz
     *
     * @return the channel processor
     */
    public static ChannelProcessor<?> getProcessor(String payloadClazz) {
        return REGISTRY.get(payloadClazz);
    }

    /**
     * retrieve registry
     *
     * @return registry
     */
    public static Map<String, ChannelProcessor<?>> getRegistry() {
        return ImmutableMap.copyOf(REGISTRY);
    }
}
