package io.zealab.kvaft.protocal;

import io.zealab.kvaft.rpc.protoc.RemoteCalls.*;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

/**
 * @author LeonWong
 */
public class ProtoBufTest {

    @Test
    public void encode() {
        Heartbeat hb = Heartbeat.newBuilder().setTimestamp(150L).build();
        System.out.println(Hex.encodeHexString(hb.toByteArray()));
    }
}
