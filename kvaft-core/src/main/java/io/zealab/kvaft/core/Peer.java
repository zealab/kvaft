package io.zealab.kvaft.core;

import io.zealab.kvaft.util.Endpoint;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Peer {

    private Endpoint endpoint;

    private int nodeId;
}
