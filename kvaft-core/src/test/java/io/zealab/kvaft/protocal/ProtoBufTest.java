package io.zealab.kvaft.protocal;

import com.google.protobuf.InvalidProtocolBufferException;
import io.zealab.kvaft.rpc.protoc.RemoteCalls.*;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

/**
 * @author LeonWong
 */
public class ProtoBufTest {

    @Test
    public void encode() throws InvalidProtocolBufferException {
        Heartbeat hb = Heartbeat.newBuilder().setTimestamp(150L).build();
        System.out.println(Hex.encodeHexString(hb.toByteArray()));
        Heartbeat hb2 = Heartbeat.parseFrom(hb.toByteArray());
        assert hb2.equals(hb);
    }
}
