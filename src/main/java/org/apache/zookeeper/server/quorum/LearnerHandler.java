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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import javax.security.sasl.SaslException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogProposalIterator;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 * Leader 将为每个 Learner 创建一个此类的实例. 与 Learner 的所有交流都由该类处理.
 */
public class LearnerHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    final LearnerMaster learnerMaster;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    String getRemoteAddress() {
        return sock == null ? "<null>" : sock.getRemoteSocketAddress().toString();
    }

    protected int version = 0x1;

    int getVersion() {
        return version;
    }

    /**
     * The packets to be sent to the learner
     * 要发送给 Learner 的数据包
     */
    // 要发送给 Learner 的 QuorumPacket 数据包队列
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();
    private final AtomicLong queuedPacketsSize = new AtomicLong();

    protected final AtomicLong packetsReceived = new AtomicLong();
    protected final AtomicLong packetsSent = new AtomicLong();

    protected final AtomicLong requestsReceived = new AtomicLong();

    protected volatile long lastZxid = -1;

    public synchronized long getLastZxid() {
        return lastZxid;
    }

    protected final Date established = new Date();

    public Date getEstablished() {
        return (Date) established.clone();
    }

    /**
     * Marker packets would be added to quorum packet queue after every
     * markerPacketInterval packets.
     * It is ok if packetCounter overflows.
     */
    private final int markerPacketInterval = 1000;
    private AtomicInteger packetCounter = new AtomicInteger();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {

        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            if (currentZxid == zxid) {
                currentTime = nextTime;
                currentZxid = nextZxid;
                nextTime = 0;
                nextZxid = 0;
            } else if (nextZxid == zxid) {
                LOG.warn(
                        "ACK for 0x{} received before ACK for 0x{}",
                        Long.toHexString(zxid),
                        Long.toHexString(currentZxid));
                nextTime = 0;
                nextZxid = 0;
            }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < learnerMaster.syncTimeout());
            }
        }

    }

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private static class MarkerQuorumPacket extends QuorumPacket {

        long time;
        MarkerQuorumPacket(long time) {
            this.time = time;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MarkerQuorumPacket that = (MarkerQuorumPacket) o;
            return time == that.time;
        }

    }

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private final BufferedInputStream bufferedInput;
    private BufferedOutputStream bufferedOutput;

    protected final MessageTracker messageTracker;

    // for test only
    protected void setOutputArchive(BinaryOutputArchive oa) {
        this.oa = oa;
    }
    protected void setBufferedOutput(BufferedOutputStream bufferedOutput) {
        this.bufferedOutput = bufferedOutput;
    }

    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /**
     * For testing purpose, force learnerMaster to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private boolean forceSnapSync = false;

    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    private boolean needOpPacket = true;

    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;

    /**
     * for sync throttling
     */
    private LearnerSyncThrottler syncThrottler = null;

    // 创建一个 LearnerHandler 对应一个 Follower 或 Observer 的连接
    LearnerHandler(Socket sock, BufferedInputStream bufferedInput, LearnerMaster learnerMaster) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.learnerMaster = learnerMaster;
        this.bufferedInput = bufferedInput;

        if (Boolean.getBoolean(FORCE_SNAP_SYNC)) {
            forceSnapSync = true;
            LOG.info("Forcing snapshot sync is enabled");
        }

        try {
            QuorumAuthServer authServer = learnerMaster.getQuorumAuthServer();
            if (authServer != null) {
                authServer.authenticate(sock, new DataInputStream(bufferedInput));
            }
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection", sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }

        this.messageTracker = new MessageTracker(MessageTracker.BUFFERED_MESSAGE_SIZE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     * 此方法将使用线程发送添加到 queuedPackets 列表的数据包
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                ServerMetrics.getMetrics().LEARNER_HANDLER_QP_SIZE.add(Long.toString(this.sid), queuedPackets.size());

                if (p instanceof MarkerQuorumPacket) {
                    MarkerQuorumPacket m = (MarkerQuorumPacket) p;
                    ServerMetrics.getMetrics().LEARNER_HANDLER_QP_TIME
                            .add(Long.toString(this.sid), (System.nanoTime() - m.time) / 1000000L);
                    continue;
                }

                queuedPacketsSize.addAndGet(-packetSize(p));
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }

                // Log the zxid of the last request, if it is a valid zxid.
                if (p.getZxid() > 0) {
                    lastZxid = p.getZxid();
                }
                oa.writeRecord(p, "packet");
                packetsSent.incrementAndGet();
                messageTracker.trackSent(p.getType());
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at {}", this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch (IOException ie) {
                        LOG.warn("Error closing socket for handler {}", this, ie);
                    }
                }
                break;
            }
        }
    }

    public static String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
                DataInputStream dis = new DataInputStream(bis);
                try {
                    long id = dis.readLong();
                    mess = " sessionid = " + id;
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }

                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            case Leader.DIFF:
                type = "DIFF";
                break;
            case Leader.TRUNC:
                type = "TRUNC";
                break;
            case Leader.SNAP:
                type = "SNAP";
                break;
            case Leader.ACKEPOCH:
                type = "ACKEPOCH";
                break;
            case Leader.SYNC:
                type = "SYNC";
                break;
            case Leader.INFORM:
                type = "INFORM";
                break;
            case Leader.COMMITANDACTIVATE:
                type = "COMMITANDACTIVATE";
                break;
            case Leader.INFORMANDACTIVATE:
                type = "INFORMANDACTIVATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     * 该线程将接收来自对等方的数据包并对其进行处理, 并且还将侦听来自新对等方的新连接.
     */
    @Override
    public void run() {
        try {
            // 添加到 Leader 的 learners 属性 (所有 Learners 的集合, 包括 Followers 和 Observers) 中
            learnerMaster.addLearnerHandler(this);
            tickOfNextAckDeadline = learnerMaster.getTickOfInitialAckDeadline();

            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            // 接收对方的消息
            QuorumPacket qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            // 只接收 FOLLOWERINFO 或者 OBSERVERINFO 请求
            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO) {
                LOG.error("First packet {} is not FOLLOWERINFO or OBSERVERINFO!", qp.toString());

                return;
            }

            if (learnerMaster instanceof ObserverMaster && qp.getType() != Leader.OBSERVERINFO) {
                throw new IOException("Non observer attempting to connect to ObserverMaster. type = " + qp.getType());
            }
            // 获取对方的 sid 和版本信息
            byte[] learnerInfoData = qp.getData();
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                if (learnerInfoData.length >= 8) {
                    this.sid = bbsid.getLong();
                }
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > learnerMaster.getQuorumVerifierVersion()) {
                        throw new IOException("Follower is ahead of the leader (has a later activated configuration)");
                    }
                }
            } else {
                this.sid = learnerMaster.getAndDecrementFollowerCounter();
            }

            String followerInfo = learnerMaster.getPeerInfo(this.sid);
            if (followerInfo.isEmpty()) {
                LOG.info(
                        "Follower sid: {} not in the current config {}",
                        this.sid,
                        Long.toHexString(learnerMaster.getQuorumVerifierVersion()));
            } else {
                LOG.info("Follower sid: {} : info : {}", this.sid, followerInfo);
            }

            if (qp.getType() == Leader.OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }

            learnerMaster.registerLearnerHandlerBean(this, sock);

            // 从对方的 zxid 解析出对方的选举轮次
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            // Leader 的选举轮次要大于对方的选举轮次
            // 等待一半以上的 Learner 发来, 才进行下一步
            long peerLastZxid;
            StateSummary ss = null;
            long zxid = qp.getZxid();
            long newEpoch = learnerMaster.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);

            // 发送 LEADERINFO, Leader 的 zxid
            // 接收对方的 ACKEPOCH, Learner 的 zxid
            // 等待一半以上的 Learner 发来, 才进行下一步
            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            } else {
                byte[] ver = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                messageTracker.trackSent(Leader.LEADERINFO);
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                messageTracker.trackReceived(ackEpochPacket.getType());
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error("{} is not ACKEPOCH", ackEpochPacket.toString());
                    return;
                }
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            }
            peerLastZxid = ss.getLastZxid();

            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases
            // 如果需要发送 TRUNC 或 DIFF, 请采取任何必要的措施
            // startForwarding() 在所有情况下都会被调用

            // 比较对方的 zxid 和当前的 CommittedLog 缓存 [minCommittedLog, maxCommittedLog] 范围, 确定同步模式: DIFF / TRUNC / SNAP
            boolean needSnap = syncFollower(peerLastZxid, learnerMaster);

            // syncs between followers and the leader are exempt from throttling because it
            // is importatnt to keep the state of quorum servers up-to-date. The exempted syncs
            // are counted as concurrent syncs though
            boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
            /* if we are not truncating or sending a diff just send a snapshot */
            /* 如果我们不 TRUNC 或发送 DIFF 仅发送 SNAP */
            if (needSnap) {
                syncThrottler = learnerMaster.getLearnerSnapSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
                try {
                    long zxidToSend = learnerMaster.getZKDatabase().getDataTreeLastProcessedZxid();
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    messageTracker.trackSent(Leader.SNAP);
                    bufferedOutput.flush();

                    LOG.info(
                            "Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                                    + "send zxid of db as 0x{}, {} concurrent snapshot sync, "
                                    + "snapshot sync was {} from throttle",
                            Long.toHexString(peerLastZxid),
                            Long.toHexString(leaderLastZxid),
                            Long.toHexString(zxidToSend),
                            syncThrottler.getSyncInProgress(),
                            exemptFromThrottle ? "exempt" : "not exempt");
                    // SNAP 模式下会执行以下操作:
                    // Dump data to peer
                    // 转储数据到 Learner
                    // 序列化 Learner 节点的 sessions 和 DataTree (即将数据写入 Learner 节点的内存中)
                    learnerMaster.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    ServerMetrics.getMetrics().SNAP_COUNT.add(1);
                }
            } else {
                syncThrottler = learnerMaster.getLearnerDiffSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
                ServerMetrics.getMetrics().DIFF_COUNT.add(1);
            }

            // 发送 NEWLEADER
            LOG.debug("Sending NEWLEADER message to {}", sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, learnerMaster.getQuorumVerifierBytes(), null);
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();

            // Start thread that blast packets in the queue to learner
            // 启动线程将把队列中的所有数据包转发给 Learner
            startSendingPackets();

            /*
             * Have to wait for the first ACK, wait until
             * the learnerMaster is ready, and only then we can
             * start processing messages.
             * 必须等待第一个 ACK, 等到 LearnerMaster 准备好, 然后我们才能开始处理消息.
             */
            // 读取对方的 ACK
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK, but received packet: {}", packetToString(qp));
                return;
            }

            LOG.debug("Received NEWLEADER-ACK message from {}", sid);

            // 在给定的 sid 后面处理 NEWLEADER, 并等待直到 Leader 收到足够的 acks (一半以上).
            learnerMaster.waitForNewLeaderAck(getSid(), qp.getZxid());

            syncLimitCheck.start();
            // sync ends when NEWLEADER-ACK is received
            syncThrottler.endSync();
            if (needSnap) {
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
            } else {
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
            }
            syncThrottler = null;

            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(learnerMaster.syncTimeout());

            /*
             * Wait until learnerMaster starts up
             */
            learnerMaster.waitForStartup();

            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            // 变动的数据包将在序列化过程中排队, 因此我们需要标记对等方何时可以真正开始使用数据.
            //
            // 发送 UPTODATE
            LOG.debug("Sending UPTODATE message to {}", sid);
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            // 不断接收 Learner (Follower / Observer) 的消息
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");
                messageTracker.trackReceived(qp.getType());

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = learnerMaster.getTickOfNextAckDeadline();

                packetsReceived.incrementAndGet();

                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                    case Leader.ACK:
                        // 收到 ACK
                        // 从 SendAckRequestProcessor 发送而来 (Leader.propose() -> SyncRequestProcessor -> SendAckRequestProcessor)

                        if (this.learnerType == LearnerType.OBSERVER) {
                            LOG.debug("Received ACK from Observer {}", this.sid);
                        }
                        syncLimitCheck.updateAck(qp.getZxid());

                        // 如果收到一半以上的 Follower 的 ack (Follower 进行 SyncRequest 后 ack),
                        // 则发送 COMMIT 给 Followers, 发送 INFORM 给 Observers.
                        learnerMaster.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                        break;
                    case Leader.PING:
                        // Process the touches
                        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
                        DataInputStream dis = new DataInputStream(bis);
                        while (dis.available() > 0) {
                            long sess = dis.readLong();
                            int to = dis.readInt();
                            learnerMaster.touch(sess, to);
                        }
                        break;
                    case Leader.REVALIDATE:
                        ServerMetrics.getMetrics().REVALIDATE_COUNT.add(1);
                        learnerMaster.revalidateSession(qp, this);
                        break;
                    case Leader.REQUEST:
                        // 收到 REQUEST 请求
                        // 从 Follower 的 FollowerRequestProcessor 的事务请求发送而来


                        bb = ByteBuffer.wrap(qp.getData());
                        sessionId = bb.getLong();
                        cxid = bb.getInt();
                        type = bb.getInt();
                        bb = bb.slice();

                        // 构造 Request
                        Request si;
                        if (type == OpCode.sync) {
                            si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                        } else {
                            si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                        }
                        si.setOwner(this);

                        // 提交 Request
                        // 转发到 Leader 的事务处理链: PrepRequestProcessor
                        learnerMaster.submitLearnerRequest(si);

                        requestsReceived.incrementAndGet();
                        break;
                    default:
                        LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                        break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    sock.close();
                } catch (IOException ie) {
                    // do nothing
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
        } catch (SyncThrottleException e) {
            LOG.error("too many concurrent sync.", e);
            syncThrottler = null;
        } catch (Exception e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
            throw e;
        } finally {
            if (syncThrottler != null) {
                syncThrottler.endSync();
                syncThrottler = null;
            }
            String remoteAddr = getRemoteAddress();
            LOG.warn("******* GOODBYE {} ********", remoteAddr);
            messageTracker.dumpToLog(remoteAddr);
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the follower
     * 启动线程将把队列中的任何数据包转发给 Follower
     */
    // 启动线程将把队列中的所有数据包转发给 Follower
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Tests need not send marker packets as they are only needed to
     * log quorum packet delays
     */
    protected boolean shouldSendMarkerPacketForLogging() {
        return true;
    }

    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     * 确定我们是否需要使用 DIFF / TRUNC / SNAP 和设置 Follower 来与 Follower 同步, 以从提交处理器接收数据包
     *
     * @param peerLastZxid
     * @param learnerMaster
     * @return true if snapshot transfer is needed.
     */
    // 比较对方的 zxid 和当前的 CommittedLog 缓存 [minCommittedLog, maxCommittedLog] 范围, 确定同步模式: DIFF / TRUNC / SNAP
    boolean syncFollower(long peerLastZxid, LearnerMaster learnerMaster) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the learnerMaster. If the same learner come
         * back to sync with learnerMaster using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         */
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued
        long currentZxid = peerLastZxid;
        boolean needSnap = true;
        ZKDatabase db = learnerMaster.getZKDatabase();
        boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
        ReentrantReadWriteLock lock = db.getLogLock();
        ReadLock rl = lock.readLock();
        try {
            rl.lock();
            // 缓存事务队列范围 (用于 Follower 与 Leader 同步时, Leader 能快速获取事务的 zxid, 判断 Follower 的同步状态)
            // 范围: [minCommittedLog, maxCommittedLog]
            long maxCommittedLog = db.getmaxCommittedLog();
            long minCommittedLog = db.getminCommittedLog();
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Learner sid: {} maxCommittedLog=0x{}"
                            + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                            + " peerLastZxid=0x{}",
                    getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid));

            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that committedLog is empty. In that case
                 * setting these value to the latest txn in learnerMaster db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and learnerMaster is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUNC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             *
             * 下面是我们要处理的情况:
             * 1. 强制发送 SnapShot 快照 (用于测试)
             * 2. 其他节点和 Leader 已经同步, 发送空 DIFF
             * 3. Follower 拥有我们未曾见过的 txn. 这可能是老 Leader, 所以我们需要发送 TRUNC.
             *    但是, 如果对方具有 newEpochZxid, 则由于 Follower 没有 txnlog, 因此我们无法发送 TRUNC
             * 4. Follower 在 commitLog 范围内或已处于同步状态.
             *    我们可能需要发送 DIFF 或 TRUNC, 具体取决于 Follower 的 zxid.
             *    如果 Follower 已经处于同步状态, 我们总是发送空的 DIFF
             * 5. Follower 错过了 commitLog. 我们将尝试使用磁盘上的 txnlog + commitLog 与 Follower 同步.
             *    如果失败, 我们将发送 SnapShot 快照
             */

            // 比较对方的 zxid 和当前的 [minCommittedLog, maxCommittedLog] 范围, 确定使用哪种同步模式: DIFF / TRUNC / SNAP
            if (forceSnapSync) {
                // CASE 1. 强制发送 SnapShot 快照 (用于测试)

                // Force learnerMaster to use snapshot to sync with follower
                // 强制 Leader 使用快照与 Follower 同步
                LOG.warn("Forcing snapshot sync - should not see this in production");
            } else if (lastProcessedZxid == peerLastZxid) {
                // CASE 2. 其他节点和 Leader 已经同步, 发送空 DIFF

                // Follower is already sync with us, send empty diff
                // Follower 已经与我们同步, 发送空 DIFF
                LOG.info(
                        "Sending DIFF zxid=0x{} for peer sid: {}",
                        Long.toHexString(peerLastZxid),
                        getSid());
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                // Follower 拥有我们未曾见过的 txn. 这可能是老 Leader, 所以我们需要发送 TRUNC.
                //         但是, 如果对方具有 newEpochZxid, 则由于 Follower 没有 txnlog, 因此我们无法发送 TRUNC

                // Newer than committedLog, send trunc and done
                // 比 commitLog 更新, 发送 TRUNC 并完成
                LOG.debug(
                        "Sending TRUNC to follower zxidToSend=0x{} for peer sid:{}",
                        Long.toHexString(maxCommittedLog),
                        getSid());

                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            } else if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                // CASE 4. Follower 在 commitLog 范围内或已处于同步状态.
                //         我们可能需要发送 DIFF 或 TRUNC, 具体取决于 Follower 的 zxid.
                //         如果 Follower 已经处于同步状态, 我们总是发送空的 DIFF

                // Follower is within commitLog range
                // Follower 在 commitLog 范围内
                LOG.info("Using committedLog for peer sid: {}", getSid());

                // 同步 (peerLastZxid, maxCommittedLog]
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                currentZxid = queueCommittedProposals(itr, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            } else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                // CASE 5. Follower 错过了 commitLog. 我们将尝试使用磁盘上的 txnlog + commitLog 与 Follower 同步.
                //         如果失败, 我们将发送 SnapShot 快照

                // Use txnlog and committedLog to sync
                // 使用 txnlog 和 commitLog 进行同步

                // Calculate sizeLimit that we allow to retrieve txnlog from disk
                long sizeLimit = db.calculateTxnLogSizeLimit();
                // This method can return empty iterator if the requested zxid
                // is older than on-disk txnlog
                // 如果请求的 zxid 早于磁盘 txnlog, 则此方法可以返回空迭代器

                // 使用磁盘上的 TxnLog 与 Follower 同步.
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: {}", getSid());

                    // 情况 a. 当 TxnLog 的 txnLogMaxzxid (最大 zxid) < minCommittedLog 时, 同步 (peerLastZxid, txnLogMaxzxid]
                    // 情况 b. 当 TxnLog 的 txnLogMaxzxid (最大 zxid) > minCommittedLog 时, 同步 (peerLastZxid, minCommittedLog]
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);

                    if (currentZxid < minCommittedLog) {
                        // 情况 a, 说明 TxnLog 与 CommitLog 无法配合完成数据同步
                        // 此处表示使用磁盘上的 txnlog 与 Follower 同步失败 (说明 txnlog 也不全)
                        // 需要发送 SnapShot 快照来完成全量同步: needSnap = true
                        LOG.info(
                                "Detected gap between end of txnlog: 0x{} and start of committedLog: 0x{}",
                                Long.toHexString(currentZxid),
                                Long.toHexString(minCommittedLog));
                        currentZxid = peerLastZxid;
                        // Clear out currently queued requests and revert
                        // to sending a snapshot.
                        queuedPackets.clear();
                        needOpPacket = true;
                    } else {
                        // 情况 b, 说明 TxnLog 与 CommitLog 可以配合完成数据同步
                        // 此处表示使用磁盘上的 txnlog 与 Follower 同步成功: 则继续使用磁盘上的 commitLog 与 Follower 同步
                        // 继续同步 (minCommittedLog, maxCommittedLog] 部分
                        LOG.debug("Queueing committedLog 0x{}", Long.toHexString(currentZxid));
                        Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                        currentZxid = queueCommittedProposals(committedLogItr, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                }
                // closing the resources
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
            } else {
                LOG.warn(
                        "Unhandled scenario for peer sid: {} maxCommittedLog=0x{}"
                                + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                                + " peerLastZxid=0x{} txnLogSyncEnabled={}",
                        getSid(),
                        Long.toHexString(maxCommittedLog),
                        Long.toHexString(minCommittedLog),
                        Long.toHexString(lastProcessedZxid),
                        Long.toHexString(peerLastZxid),
                        txnLogSyncEnabled);
            }
            if (needSnap) {
                currentZxid = db.getDataTreeLastProcessedZxid();
            }

            LOG.debug("Start forwarding 0x{} for peer sid: {}", Long.toHexString(currentZxid), getSid());
            leaderLastZxid = learnerMaster.startForwarding(this, currentZxid);
        } finally {
            rl.unlock();
        }

        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: {} fall back to use snapshot",  getSid());
            needSnap = true;
        }

        return needSnap;
    }

    /**
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     * 将已提交的 proposals 添加到数据包队列中. 将要添加到队列的数据包范围是 (peerLaxtZxid, maxZxid]
     *
     * @param itr  iterator point to the proposals
     * @param peerLastZxid  last zxid seen by the follower
     * @param maxZxid  max zxid of the proposal to queue, null if no limit
     * @param lastCommittedZxid when sending diff, we need to send lastCommittedZxid
     *        on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    // 将 (peerLaxtZxid, maxZxid] 范围内的 proposals 加入队列中 (发送到 Learners)

    // 假设 Iterator<Proposal> itr 的范围是 [500, 800], peerLastZxid 是 600
    // step 1: 循环跳过 [500, 600)
    // step 2: 当循环到 600 时, 发送 DIFF 标志
    // step 3: 循环 (600, 800] 时, 发送 Proposal 数据包和 COMMIT 标志
    // 发送内容如下:
    // Leader ----> Observer
    //        DIFF
    //        601
    //        COMMIT
    //        602
    //        COMMIT
    //        ...
    //        800
    //        COMMIT
    protected long queueCommittedProposals(Iterator<Proposal> itr, long peerLastZxid, Long maxZxid, Long lastCommittedZxid) {
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous
        // proposal Id.
        long prevProposalZxid = -1;
        while (itr.hasNext()) {
            Proposal propose = itr.next();

            long packetZxid = propose.packet.getZxid();
            // abort if we hit the limit
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }

            // skip the proposals the peer already has
            // 跳过对方已经拥有的 proposals
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }

            // If we are sending the first packet, figure out whether to trunc
            // or diff
            // 如果我们要发送第一个数据包, 请确定是 TRUNC 还是 DIFF
            if (needOpPacket) {

                // Send diff when we see the follower's zxid in our history
                if (packetZxid == peerLastZxid) {
                    LOG.info(
                            "Sending DIFF zxid=0x{}  for peer sid: {}",
                            Long.toHexString(lastCommittedZxid),
                            getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                    continue;
                }

                if (isPeerNewEpochZxid) {
                    // Send diff and fall through if zxid is of a new-epoch
                    LOG.info(
                            "Sending DIFF zxid=0x{}  for peer sid: {}",
                            Long.toHexString(lastCommittedZxid),
                            getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                } else if (packetZxid > peerLastZxid) {
                    // Peer have some proposals that the learnerMaster hasn't seen yet
                    // it may used to be a leader
                    if (ZxidUtils.getEpochFromZxid(packetZxid) != ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() + " peer zxid is from different epoch");
                        return queuedZxid;
                    }

                    LOG.info(
                            "Sending TRUNC zxid=0x{}  for peer sid: {}",
                            Long.toHexString(prevProposalZxid),
                            getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
            }

            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue
                // or there is a duplicate txn in a given iterator
                continue;
            }

            // Since this is already a committed proposal, we need to follow
            // it by a commit packet
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            queuedZxid = packetZxid;

        }

        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info(
                    "Sending TRUNC zxid=0x{}  for peer sid: {}",
                    Long.toHexString(lastCommittedZxid),
                    getSid());
            queueOpPacket(Leader.DIFF, lastCommittedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.clear();
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        learnerMaster.removeLearnerHandler(this);
        learnerMaster.unregisterLearnerHandlerBean(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the learnerMaster to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            id = learnerMaster.getLastProposed();
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }

    // 将 QuorumPacket 数据包添加到 queuedPackets (要发送给 Learner 的 QuorumPacket 数据包队列)
    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
        // Add a MarkerQuorumPacket at regular intervals.
        if (shouldSendMarkerPacketForLogging() && packetCounter.getAndIncrement() % markerPacketInterval == 0) {
            queuedPackets.add(new MarkerQuorumPacket(System.nanoTime()));
        }
        queuedPacketsSize.addAndGet(packetSize(p));
    }

    static long packetSize(QuorumPacket p) {
        /* Approximate base size of QuorumPacket: int + long + byte[] + List */
        long size = 4 + 8 + 8 + 8;
        byte[] data = p.getData();
        if (data != null) {
            size += data.length;
        }
        return size;
    }

    public boolean synced() {
        return isAlive() && learnerMaster.getCurrentTick() <= tickOfNextAckDeadline;
    }

    public synchronized Map<String, Object> getLearnerHandlerInfo() {
        Map<String, Object> info = new LinkedHashMap<>(9);
        info.put("remote_socket_address", getRemoteAddress());
        info.put("sid", getSid());
        info.put("established", getEstablished());
        info.put("queued_packets", queuedPackets.size());
        info.put("queued_packets_size", queuedPacketsSize.get());
        info.put("packets_received", packetsReceived.longValue());
        info.put("packets_sent", packetsSent.longValue());
        info.put("requests", requestsReceived.longValue());
        info.put("last_zxid", getLastZxid());

        return info;
    }

    public synchronized void resetObserverConnectionStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        requestsReceived.set(0);

        lastZxid = -1;
    }

    /**
     * For testing, return packet queue
     * @return
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }

}
