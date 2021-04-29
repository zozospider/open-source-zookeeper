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

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.zookeeper.server.DataTree;

/**
 * snapshot interface for the persistence layer.
 * implement this interface for implementing
 * snapshots.
 * 持久层的快照接口. 实现此接口以实现快照.
 */
// 快照接口
public interface SnapShot {

    /**
     * deserialize a data tree from the last valid snapshot and
     * return the last zxid that was deserialized
     * 从最后一个有效快照反序列化 data tree, 并返回最后反序列化的 zxid
     *
     * @param dt the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    // 反序列化
    long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException;

    /**
     * persist the datatree and the sessions into a persistence storage
     * 将 DataTree 和 sessions 持久化到持久性存储中
     *
     * @param dt the datatree to be serialized
     * @param sessions the session timeouts to be serialized
     * @param name the object name to store snapshot into
     * @param fsync sync the snapshot immediately after write
     * @throws IOException
     */
    // 序列化
    void serialize(DataTree dt, Map<Long, Integer> sessions, File name, boolean fsync) throws IOException;

    /**
     * find the most recent snapshot file
     * 查找最新的快照文件
     *
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;

    /**
     * get information of the last saved/restored snapshot
     * 获取上次保存 / 还原的快照的信息
     *
     * @return info of last snapshot
     */
    SnapshotInfo getLastSnapshotInfo();

    /**
     * free resources from this snapshot immediately
     * @throws IOException
     */
    void close() throws IOException;

}
