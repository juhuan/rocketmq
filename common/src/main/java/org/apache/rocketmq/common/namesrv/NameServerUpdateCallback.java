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

/**
 * 命名服务器更新回调接口
 * 用于在命名服务器地址变更时通知客户端
 */
public interface NameServerUpdateCallback {
    /**
     * 当命名服务器地址发生改变时调用
     *
     * @param namesrvAddress 新的命名服务器地址
     * @return 返回新的命名服务器地址的字符串表示
     */
    String onNameServerAddressChange(String namesrvAddress);
}

