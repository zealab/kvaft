package io.zealab.kvaft.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.core.Scanner;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;

/**
 * channel processors manager
 *
 * @author LeonWong
 */
@Slf4j
public class ChannelProcessorManager implements Scanner {

    private final static Map<String, ChannelProcessor<?>> REGISTRY = Maps.newHashMap();

    private final static String PACKAGE_SCAN = "io.zealab.kvaft";

    private ChannelProcessorManager() {

    }

    public static ChannelProcessorManager getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * get payload processor
     *
     * @param payloadClazz clazz
     *
     * @return the channel processor
     */
    public ChannelProcessor<?> getProcessor(String payloadClazz) {
        return REGISTRY.get(payloadClazz);
    }

    /**
     * retrieve registry
     *
     * @return registry
     */
    public Map<String, ChannelProcessor<?>> getRegistry() {
        return ImmutableMap.copyOf(REGISTRY);
    }

    @Override
    public void onClazzScanned(Class<?> clazz) {
        Processor kvaftProcessor = clazz.getAnnotation(Processor.class);
        if (Objects.nonNull(kvaftProcessor)) {
            Object instance;
            try {
                instance = clazz.newInstance();
            } catch (Exception e) {
                log.error("channel processor create instance failed", e);
                return;
            }
            if (instance instanceof ChannelProcessor) {
                REGISTRY.putIfAbsent(kvaftProcessor.messageClazz().toString(), (ChannelProcessor<?>) instance);
            }
        }
    }

    @Override
    public String scanPackage() {
        return PACKAGE_SCAN;
    }

    private static class SingletonHolder {
        public static ChannelProcessorManager instance = new ChannelProcessorManager();
    }
}
