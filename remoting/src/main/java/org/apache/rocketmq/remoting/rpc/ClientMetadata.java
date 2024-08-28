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
package org.apache.rocketmq.remoting.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;

public class ClientMetadata {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<>();

    public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
        if (topic == null
            || topicRouteData == null) {
            return;
        }
        TopicRouteData old = this.topicRouteTable.get(topic);
        if (!topicRouteData.topicRouteDataChanged(old)) {
            return ;
        }
        {
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
            }
        }
        {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
            if (mqEndPoints != null
                    && !mqEndPoints.isEmpty()) {
                topicEndPointsTable.put(topic, mqEndPoints);
            }
        }
    }

    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null
                && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    public void refreshClusterInfo(ClusterInfo clusterInfo) {
        if (clusterInfo == null
            || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
        }
    }

    /**
     * 根据指定的Broker名称查找主Broker的地址，BrokerId=0的Broker
     *
     * @param brokerName Broker名称，不包括集群或实例信息，用于唯一标识一个Broker
     * @return 如果找到，返回主Broker的地址；否则返回null
     */
    public String findMasterBrokerAddr(String brokerName) {
        if (!brokerAddrTable.containsKey(brokerName)) {
            return null;
        }
        // 返回Broker地址，BrokerId=0的Broker
        return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
    }

    public ConcurrentMap<String, HashMap<Long, String>> getBrokerAddrTable() {
        return brokerAddrTable;
    }

        /**
         * 根据主题路由数据，生成静态主题的MessageQueue与端点的映射
         *
         * 本方法主要用于处理静态主题的路由数据，将主题对应的队列信息和端点信息进行映射
         * 静态主题相对于动态主题，其队列数量和分布是固定的，本方法通过处理传入的路由数据
         * 实现这种静态队列的映射逻辑
         *
         * @param topic 主题名称，用于标识消息主题
         * @param route 主题的路由数据，包含了主题和其对应队列的映射信息
         * @return 返回一个ConcurrentMap，键是MessageQueue，值是对应的端点名称如果队列不存在，则值为特定的逻辑队列模拟Broker名称
         */
        public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
            // 如果路由数据为空，则直接返回空的并发HashMap
            if (route.getTopicQueueMappingByBroker() == null
                    || route.getTopicQueueMappingByBroker().isEmpty()) {
                return new ConcurrentHashMap<>();
            }

            // 初始化存储MessageQueue与端点映射的ConcurrentHashMap
            ConcurrentMap<MessageQueue, String> mqEndPointsOfBroker = new ConcurrentHashMap<>();

            // 用于按作用域分组的映射信息
            Map<String, Map<String, TopicQueueMappingInfo>> mappingInfosByScope = new HashMap<>();
            for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
                TopicQueueMappingInfo info = entry.getValue();
                String scope = info.getScope();
                if (scope != null) {
                    if (!mappingInfosByScope.containsKey(scope)) {
                        mappingInfosByScope.put(scope, new HashMap<>());
                    }
                    mappingInfosByScope.get(scope).put(entry.getKey(), entry.getValue());
                }
            }

            // 遍历按作用域分组的映射信息，处理并生成静态队列的映射关系
            for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfosByScope.entrySet()) {
                String scope = mapEntry.getKey();
                Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap =  mapEntry.getValue();

                // 存储MessageQueue与TopicQueueMappingInfo的映射
                ConcurrentMap<MessageQueue, TopicQueueMappingInfo> mqEndPoints = new ConcurrentHashMap<>();

                // 获取所有映射信息，并按Epoch值排序
                List<Map.Entry<String, TopicQueueMappingInfo>> mappingInfos = new ArrayList<>(topicQueueMappingInfoMap.entrySet());
                mappingInfos.sort((o1, o2) -> (int) (o2.getValue().getEpoch() - o1.getValue().getEpoch()));

                // 初始化最大总数和最大Epoch值
                int maxTotalNums = 0;
                long maxTotalNumOfEpoch = -1;

                // 遍历排序后的映射信息，更新最大队列数和Epoch值，同时填充MessageQueue与TopicQueueMappingInfo的映射
                for (Map.Entry<String, TopicQueueMappingInfo> entry : mappingInfos) {
                    TopicQueueMappingInfo info = entry.getValue();
                    if (info.getEpoch() >= maxTotalNumOfEpoch && info.getTotalQueues() > maxTotalNums) {
                        maxTotalNums = info.getTotalQueues();
                    }
                    for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
                        int globalId = idEntry.getKey();
                        MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(info.getScope()), globalId);
                        TopicQueueMappingInfo oldInfo = mqEndPoints.get(mq);
                        if (oldInfo == null ||  oldInfo.getEpoch() <= info.getEpoch()) {
                            mqEndPoints.put(mq, info);
                        }
                    }
                }

                // 完成静态逻辑队列的填充如果MessageQueue不存在于映射中，则使用特定的模拟Broker名称
                for (int i = 0; i < maxTotalNums; i++) {
                    MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(scope), i);
                    if (!mqEndPoints.containsKey(mq)) {
                        mqEndPointsOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
                    } else {
                        mqEndPointsOfBroker.put(mq, mqEndPoints.get(mq).getBname());
                    }
                }
            }
            // 返回填充完成的MessageQueue与端点的映射
            return mqEndPointsOfBroker;
        }

}
