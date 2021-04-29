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

import java.io.Closeable;
import java.io.IOException;
import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 * 读取事务日志的接口.
 */
// 事务日志接口
public interface TxnLog extends Closeable {

    /**
     * Setter for ServerStats to monitor fsync threshold exceed
     * @param serverStats used to update fsyncThresholdExceedCount
     */
    void setServerStats(ServerStats serverStats);

    /**
     * roll the current
     * log being appended to
     * @throws IOException
     */
    // 滚动当前日志文件, 生成一个新的日志文件
    void rollLog() throws IOException;
    /**
     * Append a request to the transaction log
     * @param hdr the transaction header
     * @param r the transaction itself
     * @return true iff something appended, otw false
     * @throws IOException
     */
    // 追加一个请求到日志文件, 包含事务头和请求记录体
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Append a request to the transaction log with a digset
     * @param hdr the transaction header
     * @param r the transaction itself
     * @param digest transaction digest
     * returns true iff something appended, otw false
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r, TxnDigest digest) throws IOException;

    /**
     * Start reading the transaction logs
     * from a given zxid
     * 开始从给定的 zxid 中读取事务日志
     *
     * @param zxid
     * @return returns an iterator to read the
     * next transaction in the logs.
     * @throws IOException
     */
    // 从 zxid 开始读取事务日志, 返回的是 zxid 之后的事务迭代器
    TxnIterator read(long zxid) throws IOException;

    /**
     * the last zxid of the logged transactions.
     * 记录的事务的最后一个 zxid.
     *
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    long getLastLoggedZxid() throws IOException;

    /**
     * truncate the log to get in sync with the
     * leader.
     * 截断日志以与 leader 同步.
     *
     * @param zxid the zxid to truncate at.
     * @throws IOException
     */
    // 截断删除大于 zxid 的事务记录 (通常在 zxid 大于 Leader 的 zxid 时调用)
    boolean truncate(long zxid) throws IOException;

    /**
     * the dbid for this transaction log.
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    long getDbId() throws IOException;

    /**
     * commit the transaction and make sure
     * they are persisted
     * 提交事务并确保它们持久化
     *
     * @throws IOException
     */
    // 刷盘, 确保持久化成功
    void commit() throws IOException;

    /**
     *
     * @return transaction log's elapsed sync time in milliseconds
     */
    long getTxnLogSyncElapsedTime();

    /**
     * close the transactions logs
     */
    void close() throws IOException;

    /**
     * Sets the total size of all log files
     */
    void setTotalLogSize(long size);

    /**
     * Gets the total size of all log files
     * 获取所有日志文件的总大小
     */
    long getTotalLogSize();

    /**
     * an iterating interface for reading
     * transaction logs.
     * 一个用于读取事务日志的迭代接口
     */
    // 多个事务日志的迭代器接口
    interface TxnIterator extends Closeable {

        /**
         * return the transaction header.
         * @return return the transaction header.
         */
        TxnHeader getHeader();

        /**
         * return the transaction record.
         * @return return the transaction record.
         */
        Record getTxn();

        /**
         * @return the digest associated with the transaction.
         */
        TxnDigest getDigest();

        /**
         * go to the next transaction record.
         * @throws IOException
         */
        boolean next() throws IOException;

        /**
         * close files and release the
         * resources
         * @throws IOException
         */
        void close() throws IOException;

        /**
         * Get an estimated storage space used to store transaction records
         * that will return by this iterator
         * @throws IOException
         */
        long getStorageSize() throws IOException;

    }

}

