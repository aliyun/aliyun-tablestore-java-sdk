package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;

public interface IChannelConnect {
    /**
     * Update the ChannelConnect corresponding to the given Channel (in memory) based on the meta-information of the Channel.
     * @param channel
     */
    void notifyStatus(Channel channel);

    /**
     * Whether ChannelConnect is closed.
     * @return
     */
    boolean closed();

    /**
     * Close the ChannelConnect.
     */
    void close();
}
