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
package org.apache.rocketmq.common.namesrv;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Map;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.utils.HttpTinyClient;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class DefaultTopAddressing implements TopAddressing {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String nsAddr;
    private String wsAddr;
    private String unitName;
    private Map<String, String> para;
    private List<TopAddressing> topAddressingList;

    public DefaultTopAddressing(final String wsAddr) {
        this(wsAddr, null);
    }

    public DefaultTopAddressing(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.topAddressingList = loadCustomTopAddressing();
    }

    public DefaultTopAddressing(final String unitName, final Map<String, String> para, final String wsAddr) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.para = para;
        this.topAddressingList = loadCustomTopAddressing();
    }

    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    private List<TopAddressing> loadCustomTopAddressing() {
        ServiceLoader<TopAddressing> serviceLoader = ServiceLoader.load(TopAddressing.class);
        Iterator<TopAddressing> iterator = serviceLoader.iterator();
        List<TopAddressing> topAddressingList = new ArrayList<>();
        if (iterator.hasNext()) {
            topAddressingList.add(iterator.next());
        }
        return topAddressingList;
    }

    @Override
    public final String fetchNSAddr() {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                String nsAddress = topAddressing.fetchNSAddr();
                if (!Strings.isNullOrEmpty(nsAddress)) {
                    return nsAddress;
                }
            }
        }
        // Return result of default implementation
        return fetchNSAddr(true, 3000);
    }

    @Override
    public void registerChangeCallBack(NameServerUpdateCallback changeCallBack) {
        if (!topAddressingList.isEmpty()) {
            for (TopAddressing topAddressing : topAddressingList) {
                topAddressing.registerChangeCallBack(changeCallBack);
            }
        }
    }

    /**
     * 获取名字服务器地址
     *
     * @param verbose 是否输出详细的错误信息
     * @param timeoutMills 超时时间，单位为毫秒
     * @return 名字服务器地址，如果获取失败则返回null
     */
    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        StringBuilder url = new StringBuilder(this.wsAddr);
        try {
            // 如果参数不为空，则构建请求的查询字符串
            if (null != para && para.size() > 0) {
                // 如果unitName不为空，则将其添加到URL中
                if (!UtilAll.isBlank(this.unitName)) {
                    url.append("-").append(this.unitName).append("?nofix=1&");
                }
                else {
                    url.append("?");
                }
                // 遍历参数，并将其添加到URL中
                for (Map.Entry<String, String> entry : this.para.entrySet()) {
                    url.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
                }
                // 移除最后一个多余的"&"
                url = new StringBuilder(url.substring(0, url.length() - 1));
            }
            // 如果参数为空，则直接使用wsAddr和unitName构建URL
            else {
                if (!UtilAll.isBlank(this.unitName)) {
                    url.append("-").append(this.unitName).append("?nofix=1");
                }
            }

            // 使用构建好的URL发起HTTP GET请求
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url.toString(), null, null, "UTF-8", timeoutMills);
            // 如果HTTP响应状态码为200，则解析响应内容
            if (200 == result.code) {
                String responseStr = result.content;
                // 如果响应内容不为空，则返回处理后的响应内容
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {
                    // 如果响应内容为空，则记录错误日志
                    LOGGER.error("fetch nameserver address is null");
                }
            } else {
                // 如果HTTP响应状态码不是200，则记录错误日志
                LOGGER.error("fetch nameserver address failed. statusCode=" + result.code);
            }
        } catch (IOException e) {
            // 如果verbose为true且发生IO异常，则记录详细的异常信息
            if (verbose) {
                LOGGER.error("fetch name server address exception", e);
            }
        }

        // 如果verbose为true且获取名字服务器地址失败，则生成并记录错误信息
        if (verbose) {
            String errorMsg =
                "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            LOGGER.warn(errorMsg);
        }
        // 获取名字服务器地址失败，返回null
        return null;
    }


    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
