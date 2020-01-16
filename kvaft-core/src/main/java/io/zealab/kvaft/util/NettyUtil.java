package io.zealab.kvaft.util;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LeonWong
 */
public class NettyUtil {

    private final static boolean EPOLL_AVAILABLE = Epoll.isAvailable();

    private static final AtomicInteger poolNumber = new AtomicInteger(1);

    /**
     * new EventLoopGroup for netty
     *
     * @param nThreads thread quantity
     * @param poolName thread pool name
     *
     * @return EventLoopGroup
     */
    public static EventLoopGroup newEventLoopGroup(int nThreads, String poolName) {
        ThreadFactory factory = r -> {
            SecurityManager s = System.getSecurityManager();
            ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            Thread t = new Thread(group, r, String.format(poolName, poolNumber.getAndIncrement()), 0);
            t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        };
        return EPOLL_AVAILABLE
                ? new EpollEventLoopGroup(nThreads, factory)
                : new NioEventLoopGroup(nThreads, factory);
    }

    /**
     * retrieve server socket channel class
     *
     * @return class
     */
    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        return EPOLL_AVAILABLE ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    /**
     * retrieve client socket channel class
     *
     * @return class
     */
    public static Class<? extends SocketChannel> getClientSocketChannelClass() {
        return EPOLL_AVAILABLE ? EpollSocketChannel.class : NioSocketChannel.class;
    }
}
