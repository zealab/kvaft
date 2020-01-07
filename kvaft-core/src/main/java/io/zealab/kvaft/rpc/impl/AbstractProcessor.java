package io.zealab.kvaft.rpc.impl;

import io.zealab.kvaft.rpc.ChannelProcessor;
import io.zealab.kvaft.rpc.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * @author LeonWong
 */
@Slf4j
public abstract class AbstractProcessor<T> implements ChannelProcessor<T> {

    public void doProcess(Message<T> msg) {

    }
}
