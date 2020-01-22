package io.zealab.kvaft.config;

import com.google.protobuf.Message;
import io.zealab.kvaft.core.ProcessorType;

import java.lang.annotation.*;

/**
 * @author LeonWong
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Processor {

    /**
     * handle type of this processor
     *
     * @return
     */
    ProcessorType handleType();

    /**
     * PB message class
     */
    Class<? extends Message> messageClazz();
}
