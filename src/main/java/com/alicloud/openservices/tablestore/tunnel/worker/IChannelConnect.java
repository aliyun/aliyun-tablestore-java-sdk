package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;

public interface IChannelConnect {
    /**
     * 根据Channel的元信息来更新该Channel(内存中)对应的ChannelConnect。
     * @param channel
     */
    void notifyStatus(Channel channel);

    /**
     * ChannelConnect是否被关闭。
     * @return
     */
    boolean closed();

    /**
     * 关闭ChannelConnect。
     */
    void close();
}
