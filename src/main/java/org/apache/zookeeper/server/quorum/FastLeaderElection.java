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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */
// 实现 Election 的快速 Leader 选举 (应用层)
public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String
            MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{}={}", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{}={}", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */
    // FastLeaderElection 的网络传输层
    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     * Notifications 是使其他对等方 (服务器) 知道给定对等方 (服务器) 已更改其投票的消息,
     * 可能是因为它已加入 Leader 选举, 或者是因为它获悉了具有更高 zxid 或相同 zxid 和更高 server id 的另一个对等 (服务器)
     *
     */
    // 接收的选票 (封装对象)
    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         * 推荐的 Leader sid
         */ long leader;

        /*
         * zxid of the proposed leader
         * 推荐的 Leader 的 zxid
         */ long zxid;

        /*
         * Epoch
         * 发送者的选举轮次
         */ long electionEpoch;

        /*
         * current state of sender
         * 发送者的状态
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         * 发送者的 sid
         * 该属性由接收方主动生成 (收到谁的选票就是谁的 sid)
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         * 推荐的 Leader 的选举轮次
         */ long peerEpoch;

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     * 对等方 (服务器) 想要发送给其他对等方 (服务器) 的消息. 这些消息既可以是 Notifications (选票), 也可以是接收通知的 Acks (消息确认).
     */
    // 发送的选票 (封装对象)
    public static class ToSend {

        enum mType {
            crequest,
            challenge,
            notification,
            ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         * 推荐的 Leader sid
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         * 推荐的 Leader 的 zxid
         */ long zxid;

        /*
         * Epoch
         * 当前服务器的选举轮次
         */ long electionEpoch;

        /*
         * Current state;
         * 当前服务器的状态
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         * 收件人地址: 即要发送出去的对方的 sid (送给谁就是谁的 sid)
         * 最终的网络包中不会封装这个属性 (Notification 中的 sid 由接收方主动生成 (收到谁的选票就是谁的 sid))
         * 此处的 sid 和 Notification 中的 sid 不一样, 此处表示要发送的目标服务器的 sid, Notification 中的 sid 为接收到的发送选票的服务器的 sid
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         * 推荐的 Leader 的选举轮次
         */ long peerEpoch;

    }

    // 要发送给其他服务器的 ToSend (Notification (选票) 和 Ack 发送对象) 队列
    LinkedBlockingQueue<ToSend> sendqueue;
    // 接收到的其他服务器的 Notification (选票) 队列
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */
    // 包装 WorkerReceiver 和 WorkerSender
    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        // 选票接收线程
        class WorkerReceiver extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            // 不断从 QuorumCnxManager 网络层获取消息解析成 Notification 后添加到 recvqueue (接收队列)
            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        // 调用 QuorumCnxManager 网络层, 获取消息
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        // 创建 Notification (选票) 后添加到 recvqueue (接收队列)
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {
                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */

                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                            response.sid, capacity, version, configLength));
                                }

                                byte[] b = new byte[configLength];
                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}",
                                                    self.getId(),
                                                    Long.toHexString(rqv.getVersion()),
                                                    Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                    response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         * 如果来自非投票服务器 (例如 observer 或非投票 follower), 请立即做出响应.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(
                                    ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            // 设置 Notification (选票) 属性
                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                    "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, "
                                            + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                    self.getPeerState(),
                                    n.sid,
                                    n.state,
                                    n.leader,
                                    Long.toHexString(n.electionEpoch),
                                    Long.toHexString(n.peerEpoch),
                                    Long.toHexString(n.zxid),
                                    Long.toHexString(n.version),
                                    (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // 如果自己是 LOOKING 状态, 说明正在进行 Leader 选举

                                // 添加到 recvqueue (接收队列)
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 * 如果发送此消息的对等方也在寻找并且其逻辑时钟落后, 则发送回通知.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())) {
                                    // 如果对方是 LOOKING 状态, 且选举轮次落后我的, 则发送回通知.
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                // 如果自己是非 LOOKING 状态 (LEADING / FOLLOWING), 说明已经完成 Leader 选举了, 则把我已经选出的 Leader 选票发送回去
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                // 拿到我已经选出的 Leader 选票
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    // 如果对方是 LOOKING 状态, 则将我已经选出的 Leader 选票发送回去

                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                            "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                            self.getId(),
                                            response.sid,
                                            Long.toHexString(current.getZxid()),
                                            current.getId(),
                                            Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        // 选票发送线程
        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            // 不断从 sendqueue (发送队列) 获取 ToSend 消息并发送到 QuorumCnxManager 网络层
            public void run() {
                while (!stop) {
                    try {
                        // 从 sendqueue (发送队列) 获取 ToSend 对象
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }
                        // 将 ToSend 对象发送到 QuorumCnxManager 网络层
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            // 将 ToSend 对象发送到 QuorumCnxManager 网络层
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);
                // 调用 QuorumCnxManager 网络层, 将数据发送给某台 sid
                manager.toSend(m.sid, requestBuffer);

            }

        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        // 初始化 WorkerSender (投票发送线程) 和 WorkerReceiver (投票接收线程)
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         *
         */
        // 启动 WorkerSender (选票发送线程) and WorkerReceiver (选票接收线程)
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    // 当前推荐的 Leader 的 sid
    long proposedLeader;
    // 当前推荐的 Leader 的 zxid
    long proposedZxid;
    // 当前推荐的 Leader 的选举轮次
    long proposedEpoch;

    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    // 创建 FastLeaderElection (快速选举算法)
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        // 初始化 FastLeaderElection 的网络传输层
        this.manager = manager;
        // 初始化 sendqueue (投票发送线程) 和 recvqueue (投票接收线程)
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    // 初始化 sendqueue (发送队列) 和 recvqueue (接收队列)
    // 初始化 WorkerSender (投票发送线程) 和 WorkerReceiver (投票接收线程)
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    // 启动 FastLeaderElection
    public void start() {
        // 启动 WorkerSender (选票发送线程) and WorkerReceiver (选票接收线程)
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug(
                "About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                v.getId(),
                Long.toHexString(v.getZxid()),
                self.getId(),
                self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;
    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     * 在我们的投票发生变化时向所有同伴发送通知
     */
    // 把当前 Notifications (选票) 发送给所有选举服务器
    private void sendNotifications() {
        // 循环配置的选举服务器
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // 封装 ToSend 发送对象
            // sid 表示接收选票的服务器的 sid, 所以每次循环会不一样
            ToSend notmsg = new ToSend(
                    ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch,
                    qv.toString().getBytes());

            LOG.debug(
                    "Sending Notification: {} (n.leader), 0x{} (n.zxid), 0x{} (n.round), {} (recipient),"
                            + " {} (myid), 0x{} (n.peerEpoch) ",
                    proposedLeader,
                    Long.toHexString(proposedZxid),
                    Long.toHexString(logicalclock.get()),
                    sid,
                    self.getId(),
                    Long.toHexString(proposedEpoch));

            // 添加到 sendqueue (发送队列)
            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     */
    // 选票 PK
    // PK 的优先级依次为: epoch, zxid, sid
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug(
                "id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}",
                newId,
                curId,
                Long.toHexString(newZxid),
                Long.toHexString(curZxid));

        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        // PK 的优先级依次为: epoch, zxid, sid
        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                && ((newZxid > curZxid)
                || ((newZxid == curZxid)
                && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    // 获取投票箱中和当前推荐的 Leader 同样的选票集合
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != self.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    // 更新选票
    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug(
                "Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)",
                leader,
                Long.toHexString(zxid),
                proposedLeader,
                Long.toHexString(proposedZxid));

        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     * 根据给定的推荐的 Leader 更新对等状态. 如果它成为 Leader, 也要对投票箱其进行更新.
     */
    // 如果选出来的 Leader 是自己, 就将自己设置为为 LEADING, 如果是其他服务器, 就将自己设置为 FOLLOWING / OBSERVING
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();
        self.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    // Leader 选举
    // 1. 投给自己
    // 2. 不断接收其他服务器的选票 (loop):
    //    a. 将接收到的选票与当前选票进行 PK
    //    b. 更新投票
    //    c. 发送投票
    //    d. 统计投票箱
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce on whether a majority
             * of participants has voted for it.
             */
            // 投票箱
            // key: sid -> value: Vote (投票信息)
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             */
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = minNotificationInterval;

            synchronized (this) {
                logicalclock.incrementAndGet();
                // 更新选票: 投给自己
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info(
                    "New election. My id = {}, proposed zxid=0x{}",
                    self.getId(),
                    Long.toHexString(proposedZxid));
            // 把当前 Notifications (选票) 发送给所有选举服务器
            sendNotifications();

            SyncedLearnerTracker voteSet;

            /*
             * Loop in which we exchange notifications until we find a leader
             */
            // 循环交换选票直到找到 Leader
            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // 从 recvqueue (接收队列) 中获取选票
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if (n == null) {
                    // 拿到的 Notification (选票) 为 null
                    // 首次投给自己的投票被取出后, 后面的循环如果拿到的 Notification (选票) 为 null, 则有可能当前服务器还没有与其他服务器建立连接

                    if (manager.haveDelivered()) {
                        // 也可能确实是没有数据发送过来
                        sendNotifications();
                    } else {
                        // 连接所有服务器
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = Math.min(tmpTimeOut, maxNotificationInterval);
                    LOG.info("Notification time out: {}", notTimeout);

                } else if (validVoter(n.sid) && validVoter(n.leader)) {
                    // 拿到的 Notification (选票) 不为 null 且有效

                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    switch (n.state) {
                        case LOOKING:
                            // 如果接收的选票状态为 LOOKING 说明对方也在寻找 Leader

                            if (getInitLastLoggedZxid() == -1) {
                                LOG.debug("Ignoring notification as our zxid is -1");
                                break;
                            }
                            if (n.zxid == -1) {
                                LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                                break;
                            }
                            // If notification > current, replace and send messages out
                            if (n.electionEpoch > logicalclock.get()) {
                                // 如果接收的选票的 epoch (选举轮次) 大于当前的, 则清空投票箱, 并 PK 选票, 发送选票

                                logicalclock.set(n.electionEpoch);
                                // 清空投票箱
                                recvset.clear();
                                // 接收的选票和当前服务器投自己的选票进行 PK
                                // 对 PK 后的选票进行更新
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    // 如果接收的选票更厉害, 则更新为对方的
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    // 否则更新为自己的
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }
                                // 把当前 Notifications (选票) 发送给所有选举服务器
                                sendNotifications();

                            } else if (n.electionEpoch < logicalclock.get()) {
                                // 如果接收的选票的 epoch (选举轮次) 小于当前的, 则不做处理

                                LOG.debug(
                                        "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                        Long.toHexString(n.electionEpoch),
                                        Long.toHexString(logicalclock.get()));
                                break;

                            } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                // 如果接收的选票的 epoch (选举轮次) 等于当前的
                                // 选票 PK
                                // 如果接收的选票更厉害, 则更新为对方的, 然后发送选票

                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            LOG.debug(
                                    "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                                    n.sid,
                                    n.leader,
                                    Long.toHexString(n.zxid),
                                    Long.toHexString(n.electionEpoch));

                            // don't care about the version if it's in LOOKING state
                            // 将本次选票保存到投票箱
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            // 统计选票:
                            // 将当前推荐的 Leader 和投票箱中的选票进行统计, 判断是否能选出真正的 Leader
                            // 获取投票箱中和当前推荐的 Leader 同样的选票集合
                            voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));
                            // 判断和当前推荐的 Leader 同样的选票是否满足了过半机制
                            if (voteSet.hasAllQuorums()) {
                                // 如果满足过半机制, 则可暂时认为选出了 Leader

                                // Verify if there is any change in the proposed leader
                                // 确认所推荐的 Leader 是否有任何变化
                                // 保险机制:
                                // 如果 recvqueue (接收队列) 中还有选票, 且接收的选票比当前选票还要优秀 (通过选票 PK), 那么会再次进入上层 while 交换选票 PK 循环, 重新确认 Leader.
                                // 如果接收的所有选票都不够优秀 (通过选票 PK), 则结束此次保险机制.
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    // 选票 PK
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 * 一旦我们没有从接收队列中读取任何新的相关消息, 则该断定为真
                                 */
                                if (n == null) {
                                    // 如果选出来的 Leader 是自己, 就将自己设置为为 LEADING, 如果是其他服务器, 就将自己设置为 FOLLOWING / OBSERVING
                                    setPeerState(proposedLeader, voteSet);
                                    // 将本服务器最终的投票返回, 结束 lookForLeader() 方法
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;
                        case OBSERVING:
                            // 如果接收的选票状态为 OBSERVING, 则继续下一轮 while 交换选票 PK 循环, 重新确认 Leader.
                            LOG.debug("Notification from observer: {}", n.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                            // 如果接收的选票状态为 FOLLOWING / LEADING, 说明其他服务器已经完成了 Leader 选举, 则执行以下逻辑:
                            // 1. 统计选票
                            // 2. 更改自己的状态, 并将本服务器最终的投票 (即为接收到的选票) 返回, 结束 lookForLeader() 方法

                            /*
                             * Consider all notifications from the same epoch
                             * together.
                             */
                            if (n.electionEpoch == logicalclock.get()) {
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                                voteSet = getVoteTracker(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                                if (voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {
                                    setPeerState(n.leader, voteSet);
                                    Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }

                            /*
                             * Before joining an established ensemble, verify that
                             * a majority are following the same leader.
                             *
                             * Note that the outofelection map also stores votes from the current leader election.
                             * See ZOOKEEPER-1732 for more information.
                             */
                            outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            voteSet = getVoteTracker(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

                            if (voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    setPeerState(n.leader, voteSet);
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: {} (n.state), {}(n.sid)", n.state, n.sid);
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
