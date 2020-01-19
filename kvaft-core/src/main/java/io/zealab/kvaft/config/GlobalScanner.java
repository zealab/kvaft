package io.zealab.kvaft.config;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import io.zealab.kvaft.core.Initializer;
import io.zealab.kvaft.core.Scanner;
import io.zealab.kvaft.rpc.ChannelProcessorManager;
import io.zealab.kvaft.rpc.protoc.ProtocHandleManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * global scanner
 *
 * @author LeonWong
 */
@Slf4j
public class GlobalScanner implements Initializer {

    private final static ImmutableSet<? extends Scanner> SCANNERS = ImmutableSet.of(
            ProtocHandleManager.getInstance(), ChannelProcessorManager.getInstance()
    );

    private final static String GLOBAL_SCAN = "io.zealab.kvaft";

    private final static ClassLoader cl = GlobalScanner.class.getClassLoader();

    @Override
    public void init() {
        // scanning packages and loading class
        scanPackage();
    }

    private void scanPackage() {
        try {
            ImmutableSet<ClassPath.ClassInfo> classInfos = ClassPath.from(cl).getTopLevelClassesRecursive(GLOBAL_SCAN);
            for (ClassPath.ClassInfo classInfo : classInfos) {
                Class<?> clazz = classInfo.load();
                SCANNERS.stream().filter(e -> clazz.getName().startsWith(e.scanPackage())).forEach(
                        e -> e.onClazzScanned(clazz)
                );
            }
        } catch (IOException e) {
            log.error("initializer scan package failed", e);
        }
    }
}
