/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jute;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that is implemented by generated classes.
 * Zookeeper 的序列化接口, 如果某个类需要进行序列化 / 反序列化 (通常用于客户端, 服务端, 不同节点间的网络传输), 需要实现该类
 * 实现类如: CreateRequest, CreateResponse, GetDataRequest, GetDataResponse 等
 */
@InterfaceAudience.Public
public interface Record {
    // 具体如何进行序列化 (输出)
    void serialize(OutputArchive archive, String tag) throws IOException;
    // 具体如何进行反序列化 (输入)
    void deserialize(InputArchive archive, String tag) throws IOException;
}
