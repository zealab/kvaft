package io.zealab.kvaft.util;

import lombok.Builder;
import lombok.Data;

/**
 * @author LeonWong
 */
@Data
@Builder
public class Endpoint {

    private String ip;

    private int port;

    @Override
    public String toString() {
        return String.format("%s:%d", ip, port);
    }
}
