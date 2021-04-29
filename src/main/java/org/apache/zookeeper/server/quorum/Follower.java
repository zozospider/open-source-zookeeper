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

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner {

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;

    ObserverMaster om;

    Follower(QuorumPeer self, FollowerZooKeeperServer zk) {
        this.self = self;
        this.zk = zk;
        this.fzk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:").append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    // Follower 跟随 Leader 的主要方法 (此方法会阻塞)
    void followLeader() throws InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        ServerMetrics.getMetrics().ELECTION_TIME.add(electionTimeTaken);
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - {} {}", electionTimeTaken, QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);

        long connectionTime = 0;
        boolean completedSync = false;

        try {
            self.setZabState(QuorumPeer.ZabState.DISCOVERY);
            // 找到 Leader
            QuorumServer leaderServer = findLeader();
            try {
                // 连接 Leader
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                connectionTime = System.currentTimeMillis();

                // 注册 Leader, 获取 Leader 的 zxid
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);
                if (self.isReconfigStateChange()) {
                    throw new Exception("learned about role change");
                }
                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch "
                            + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch "
                            + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                long startTime = Time.currentElapsedTime();
                try {
                    self.setLeaderAddressAndId(leaderServer.addr, leaderServer.getId());
                    self.setZabState(QuorumPeer.ZabState.SYNCHRONIZATION);
                    // 与 Leader 数据同步
                    syncWithLeader(newEpochZxid);
                    self.setZabState(QuorumPeer.ZabState.BROADCAST);
                    completedSync = true;
                } finally {
                    long syncTime = Time.currentElapsedTime() - startTime;
                    ServerMetrics.getMetrics().FOLLOWER_SYNC_TIME.add(syncTime);
                }
                if (self.getObserverMasterPort() > 0) {
                    LOG.info("Starting ObserverMaster");

                    om = new ObserverMaster(self, fzk, self.getObserverMasterPort());
                    om.start();
                } else {
                    om = null;
                }

                // 阻塞
                // 不断循环读取 Leader 的消息, 并处理消息
                // create a reusable packet to reduce gc impact
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    // 读取 Leader 的数据包
                    readPacket(qp);
                    // 处理 Leader 的数据包
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                closeSocket();

                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            if (om != null) {
                om.stop();
            }
            zk.unregisterJMX(this);

            if (connectionTime != 0) {
                long connectionDuration = System.currentTimeMillis() - connectionTime;
                LOG.info(
                        "Disconnected from leader (with address: {}). Was connected for {}ms. Sync state: {}",
                        leaderAddr,
                        connectionDuration,
                        completedSync);
                messageTracker.dumpToLog(leaderAddr.toString());
            }
        }
    }

    /**
     * Examine the packet received in qp and dispatch based on its contents.
     * 检查在 qp 中接收到的数据包, 并根据其内容进行分发.
     *
     * @param qp
     * @throws IOException
     */
    // 处理 Leader 的数据包
    protected void processPacket(QuorumPacket qp) throws Exception {
        switch (qp.getType()) {
            case Leader.PING:
                ping(qp);
                break;
            case Leader.PROPOSAL:
                // 事务消息
                // 将消息传入请求处理链: SyncRequestProcessor

                ServerMetrics.getMetrics().LEARNER_PROPOSAL_RECEIVED_COUNT.add(1);
                TxnLogEntry logEntry = SerializeUtils.deserializeTxn(qp.getData());
                TxnHeader hdr = logEntry.getHeader();
                Record txn = logEntry.getTxn();
                TxnDigest digest = logEntry.getDigest();
                if (hdr.getZxid() != lastQueued + 1) {
                    LOG.warn(
                            "Got zxid 0x{} expected 0x{}",
                            Long.toHexString(hdr.getZxid()),
                            Long.toHexString(lastQueued + 1));
                }
                lastQueued = hdr.getZxid();

                if (hdr.getType() == OpCode.reconfig) {
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
                    self.setLastSeenQuorumVerifier(qv, true);
                }

                // 处理 Request 消息, 将消息传入请求处理链 SyncRequestProcessor
                fzk.logRequest(hdr, txn, digest);

                if (hdr != null) {
                    /*
                     * Request header is created only by the leader, so this is only set
                     * for quorum packets. If there is a clock drift, the latency may be
                     * negative. Headers use wall time, not CLOCK_MONOTONIC.
                     */
                    long now = Time.currentWallTime();
                    long latency = now - hdr.getTime();
                    if (latency >= 0) {
                        ServerMetrics.getMetrics().PROPOSAL_LATENCY.add(latency);
                    }
                }
                if (om != null) {
                    final long startTime = Time.currentElapsedTime();
                    om.proposalReceived(qp);
                    ServerMetrics.getMetrics().OM_PROPOSAL_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
                }
                break;
            case Leader.COMMIT:
                // 提交消息
                // 将消息传入请求处理链: CommitProcessor

                ServerMetrics.getMetrics().LEARNER_COMMIT_RECEIVED_COUNT.add(1);

                // 处理 Request 消息, 将消息传入请求处理链 CommitProcessor
                fzk.commit(qp.getZxid());
                if (om != null) {
                    final long startTime = Time.currentElapsedTime();
                    om.proposalCommitted(qp.getZxid());
                    ServerMetrics.getMetrics().OM_COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - startTime);
                }
                break;

            case Leader.COMMITANDACTIVATE:
                // 提交消息
                // 将消息传入请求处理链: CommitProcessor

                // get the new configuration from the request
                Request request = fzk.pendingTxns.element();
                SetDataTxn setDataTxn = (SetDataTxn) request.getTxn();
                QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));

                // get new designated leader from (current) leader's message
                ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                long suggestedLeaderId = buffer.getLong();
                final long zxid = qp.getZxid();
                boolean majorChange = self.processReconfig(qv, suggestedLeaderId, zxid, true);

                // commit (writes the new config to ZK tree (/zookeeper/config)
                // 处理 Request 消息, 将消息传入请求处理链 CommitProcessor
                fzk.commit(zxid);

                if (om != null) {
                    om.informAndActivate(zxid, suggestedLeaderId);
                }
                if (majorChange) {
                    throw new Exception("changes proposed in reconfig");
                }
                break;
            case Leader.UPTODATE:
                LOG.error("Received an UPTODATE message after Follower started");
                break;
            case Leader.REVALIDATE:
                if (om == null || !om.revalidateLearnerSession(qp)) {
                    revalidate(qp);
                }
                break;
            case Leader.SYNC:
                fzk.sync();
                break;
            default:
                LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
                break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        try {
            synchronized (fzk) {
                return fzk.getZxid();
            }
        } catch (NullPointerException e) {
            LOG.warn("error getting zxid", e);
        }
        return -1;
    }

    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    public Integer getSyncedObserverSize() {
        return om == null ? null : om.getNumActiveObservers();
    }

    public Iterable<Map<String, Object>> getSyncedObserversInfo() {
        if (om != null && om.getNumActiveObservers() > 0) {
            return om.getActiveObservers();
        }
        return Collections.emptySet();
    }

    public void resetObserverConnectionStats() {
        if (om != null && om.getNumActiveObservers() > 0) {
            om.resetObserverConnectionStats();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("shutdown Follower");
        super.shutdown();
    }

}
