package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;

import java.util.concurrent.ExecutorService;

public class ResourceManager {

    private final ClientResources resources;
    private final boolean owned;
    private final ResourceManager sharedInstance;

    /**
     * create a resource manager which creates and owns resources.
     * @param configuration
     */
    public ResourceManager(ClientConfiguration configuration) {
        this(configuration, null);
    }

    /**
     * create a resource manager which creates and owns resources.
     * @param configuration
     * @param callbackExecutor
     */
    public ResourceManager(ClientConfiguration configuration, ExecutorService callbackExecutor) {
        if (configuration == null) {
            configuration = new ClientConfiguration();
        }
        this.resources = new ClientResources(configuration, callbackExecutor);
        this.owned = true;

        // pre-create a shared resource manager
        this.sharedInstance = new ResourceManager(this.resources);
    }

    /**
     * create a resource manager which shares other manager's resources.
     * @param resources
     */
    public ResourceManager(ClientResources resources) {
        this.resources = resources;
        this.owned = false;
        this.sharedInstance = null;
    }

    /**
     * get a shared resource manager
     * @return
     */
    public ResourceManager sharedResourceManager() {
        if (!owned) {
            return this;
        }
        return sharedInstance;
    }

    public ClientResources getResources() {
        return this.resources;
    }

    public void shutdown() {
        if (owned) {
            resources.shutdown();
        }
    }

}
