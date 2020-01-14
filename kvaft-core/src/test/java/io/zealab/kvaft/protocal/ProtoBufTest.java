package io.zealab.kvaft.protocal;

import com.google.protobuf.InvalidProtocolBufferException;
import io.zealab.kvaft.config.GlobalInitializer;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls.Heartbeat;
import io.zealab.kvaft.rpc.protoc.codec.KvaftProtocolSerializer;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author LeonWong
 */
public class ProtoBufTest {

    private KvaftProtocolSerializer serializer = KvaftProtocolSerializer.getInstance();

    @Test
    public void encode() throws InvalidProtocolBufferException {
        Heartbeat hb = Heartbeat.newBuilder().setTimestamp(150L).build();
        System.out.println(Hex.encodeHexString(hb.toByteArray()));
        Heartbeat hb2 = Heartbeat.newBuilder().build().getParserForType().parseFrom(hb.toByteArray());
        assert hb2.equals(hb);
    }

    @Test
    public void serializer() {
        GlobalInitializer initializer = new GlobalInitializer();
        initializer.init();
        Heartbeat hb = Heartbeat.newBuilder().setTimestamp(150L).build();
        KvaftMessage<Heartbeat> message = KvaftMessage.<Heartbeat>builder().payload(hb).requestId(201920391203L).node(1).from("127.0.0.1:8901").build();
        ByteBuffer byteBuffer = serializer.encode(message);
        byteBuffer.rewind();
        List<KvaftMessage<?>> hb2 = serializer.decode(byteBuffer);
        assert hb.equals(hb2.get(0).payload());
    }
}
