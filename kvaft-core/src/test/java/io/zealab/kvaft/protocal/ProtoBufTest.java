package io.zealab.kvaft.protocal;

import com.google.protobuf.InvalidProtocolBufferException;
import io.zealab.kvaft.config.GlobalScanner;
import io.zealab.kvaft.rpc.protoc.KvaftMessage;
import io.zealab.kvaft.rpc.protoc.RemoteCalls;
import io.zealab.kvaft.rpc.protoc.codec.CodecFactory;
import io.zealab.kvaft.rpc.protoc.codec.KvaftProtocolCodec;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author LeonWong
 */
public class ProtoBufTest {

    private KvaftProtocolCodec serializer = CodecFactory.getInstance();

    @Test
    public void encode() throws InvalidProtocolBufferException {
        RemoteCalls.Heartbeat hb = RemoteCalls.Heartbeat.newBuilder().setTimestamp(150L).build();
        RemoteCalls.Heartbeat hb2 = RemoteCalls.Heartbeat.newBuilder().build().getParserForType().parseFrom(hb.toByteArray());
        assert hb2.equals(hb);
    }

    @Test
    public void serializer() {
        GlobalScanner initializer = new GlobalScanner();
        initializer.init();
        RemoteCalls.Heartbeat hb = RemoteCalls.Heartbeat.newBuilder().setTimestamp(150L).build();
        KvaftMessage<RemoteCalls.Heartbeat> message = KvaftMessage.<RemoteCalls.Heartbeat>builder().payload(hb).requestId(201920391203L).build();
        ByteBuffer byteBuffer = serializer.encode(message);
        byteBuffer.rewind();
        List<KvaftMessage<?>> hb2 = serializer.decode(byteBuffer);
        assert hb.equals(hb2.get(0).payload());
    }
}
