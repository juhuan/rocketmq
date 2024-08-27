package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

/**
 * 内部生产者接口，提供消息发送和其他相关功能
 */
public interface MQProducerInner {
    /**
     * 获取当前生产者可以发布的主题列表
     *
     * @return 可发布主题的集合
     */
    Set<String> getPublishTopicList();

    /**
     * 检查指定主题的发布列表是否需要更新
     *
     * @param topic 要检查的主题
     * @return 如果发布列表需要更新，则返回true；否则返回false
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    /**
     * 获取用于检查事务状态的监听器
     *
     * @return 事务检查监听器
     */
    TransactionCheckListener checkListener();

    /**
     * 获取事务监听器
     *
     * @return 事务监听器
     */
    TransactionListener getCheckListener();

    /**
     * 检查事务状态，通过给定的远程地址、消息和请求头
     *
     * @param addr 远程地址
     * @param msg 消息内容
     * @param checkRequestHeader 检查事务状态的请求头
     */
    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新指定主题的发布信息
     *
     * @param topic 主题名称
     * @param info 新的发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    /**
     * 检查是否启用了单元模式
     *
     * @return 如果启用了单元模式，则返回true；否则返回false
     */
    boolean isUnitMode();
}
