package io.zealab.kvaft.config;

import com.google.protobuf.Message;

import java.lang.annotation.*;

/**
 * @author LeonWong
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Processor {

    /**
     * PB message class
     */
    Class<? extends Message> messageClazz();
}
