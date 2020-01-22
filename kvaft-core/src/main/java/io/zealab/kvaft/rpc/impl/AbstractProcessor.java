package io.zealab.kvaft.rpc.impl;

import com.google.protobuf.Message;
import io.zealab.kvaft.config.Processor;
import io.zealab.kvaft.rpc.ChannelProcessor;
import lombok.extern.slf4j.Slf4j;

/**
 * server side's abstract processor
 *
 * @author LeonWong
 */
@Slf4j
public abstract class AbstractProcessor implements ChannelProcessor {

    /**
     * assert type match
     *
     * @param payload payload
     */
    @Override
    public void assertMatch(Message payload) {
        Processor processor = this.getClass().getAnnotation(Processor.class);
        if (null != processor) {
            boolean isMatch = processor.messageClazz().getName().equals(payload.getClass().getName());
            if (!isMatch) {
                throw new IllegalArgumentException("payload is not match here");
            }
        }
    }
}
