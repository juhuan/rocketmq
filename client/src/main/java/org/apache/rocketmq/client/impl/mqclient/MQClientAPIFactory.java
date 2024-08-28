/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.mqclient;

import com.google.common.base.Strings;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;

/**
 * MQClientAPIFactory类用于创建和管理MQ客户端API的工厂类
 * 它实现了StartAndShutdown接口，提供客户端的启动和关闭方法
 */
public class MQClientAPIFactory implements StartAndShutdown {

    // 客户端数组，用于存储创建的MQ客户端API实例
    private MQClientAPIExt[] clients;
    // 客户端名称前缀，用于区分不同的客户端实例
    private final String namePrefix;
    // 客户端数量，决定要创建的客户端实例的数量
    private final int clientNum;
    // 客户端远程处理进程，用于处理客户端的远程通信
    private final ClientRemotingProcessor clientRemotingProcessor;
    // RPC钩子，用于扩展和定制RPC调用行为
    private final RPCHook rpcHook;
    // 定时任务的线程池，用于执行定时任务
    private final ScheduledExecutorService scheduledExecutorService;
    // 名称服务器访问配置，存储名称服务器的地址或域名配置
    private final NameserverAccessConfig nameserverAccessConfig;

    /**
     * 构造函数，初始化MQClientAPIFactory实例
     *
     * @param nameserverAccessConfig 名称服务器访问配置
     * @param namePrefix 客户端名称前缀
     * @param clientNum 客户端数量
     * @param clientRemotingProcessor 客户端远程处理进程
     * @param rpcHook RPC钩子
     * @param scheduledExecutorService 定时任务的线程池
     */
    public MQClientAPIFactory(NameserverAccessConfig nameserverAccessConfig, String namePrefix, int clientNum,
        ClientRemotingProcessor clientRemotingProcessor,
        RPCHook rpcHook, ScheduledExecutorService scheduledExecutorService) {
        this.nameserverAccessConfig = nameserverAccessConfig;
        this.namePrefix = namePrefix;
        this.clientNum = clientNum;
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.rpcHook = rpcHook;
        this.scheduledExecutorService = scheduledExecutorService;

        this.init();
    }

    /**
     * 初始化方法，设置系统属性和名称服务器地址
     */
    protected void init() {
        System.setProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false");
        if (StringUtils.isEmpty(nameserverAccessConfig.getNamesrvDomain())) {
            if (Strings.isNullOrEmpty(nameserverAccessConfig.getNamesrvAddr())) {
                throw new RuntimeException("The configuration item NamesrvAddr is not configured");
            }
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, nameserverAccessConfig.getNamesrvAddr());
        } else {
            System.setProperty("rocketmq.namesrv.domain", nameserverAccessConfig.getNamesrvDomain());
            System.setProperty("rocketmq.namesrv.domain.subgroup", nameserverAccessConfig.getNamesrvDomainSubgroup());
        }
    }

    /**
     * 获取一个MQ客户端API实例
     * 如果有多个客户端，随机选择一个
     *
     * @return MQClientAPIExt实例
     */
    public MQClientAPIExt getClient() {
        if (clients.length == 1) {
            return this.clients[0];
        }
        int index = ThreadLocalRandom.current().nextInt(this.clients.length);
        return this.clients[index];
    }

    /**
     * 启动所有MQ客户端API实例
     * 根据客户端数量创建相应数量的客户端实例
     *
     * @throws Exception 如果启动过程中发生错误
     */
    @Override
    public void start() throws Exception {
        this.clients = new MQClientAPIExt[this.clientNum];

        for (int i = 0; i < this.clientNum; i++) {
            clients[i] = createAndStart(this.namePrefix + "N_" + i);
        }
    }

    /**
     * 关闭所有MQ客户端API实例
     *
     * @throws Exception 如果关闭过程中发生错误
     */
    @Override
    public void shutdown() throws Exception {
        for (int i = 0; i < this.clientNum; i++) {
            clients[i].shutdown();
        }
    }

    /**
     * 创建并启动一个MQ客户端API实例
     *
     * @param instanceName 客户端实例的名称
     * @return 创建并启动的MQClientAPIExt实例
     */
    protected MQClientAPIExt createAndStart(String instanceName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(instanceName);
        clientConfig.setDecodeReadBody(true);
        clientConfig.setDecodeDecompressBody(false);

        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setDisableCallbackExecutor(true);

        MQClientAPIExt mqClientAPIExt = new MQClientAPIExt(clientConfig, nettyClientConfig,
            clientRemotingProcessor,
            rpcHook);

        if (!mqClientAPIExt.updateNameServerAddressList()) {
            mqClientAPIExt.fetchNameServerAddr();
            this.scheduledExecutorService.scheduleAtFixedRate(
                mqClientAPIExt::fetchNameServerAddr,
                Duration.ofSeconds(10).toMillis(),
                Duration.ofMinutes(2).toMillis(),
                TimeUnit.MILLISECONDS
            );
        }
        mqClientAPIExt.start();
        return mqClientAPIExt;
    }
}
