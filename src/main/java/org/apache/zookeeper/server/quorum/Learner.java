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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocket;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.TxnLogEntry;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share
 * a good deal of code which is moved into Peer to avoid duplication.
 */
public class Learner {

    static class PacketInFlight {

        TxnHeader hdr;
        Record rec;
        TxnDigest digest;

    }

    QuorumPeer self;
    LearnerZooKeeperServer zk;

    protected BufferedOutputStream bufferedOutput;

    protected Socket sock;
    protected MultipleAddresses leaderAddr;

    /**
     * Socket getter
     * @return
     */
    public Socket getSocket() {
        return sock;
    }

    LearnerSender sender = null;
    // 连接 Leader 的网络输入输出流
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;

    private static final int BUFFERED_MESSAGE_SIZE = 10;
    protected final MessageTracker messageTracker = new MessageTracker(BUFFERED_MESSAGE_SIZE);

    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    /**
     * Time to wait after connection attempt with the Leader or LearnerMaster before this
     * Learner tries to connect again.
     */
    private static final int leaderConnectDelayDuringRetryMs = Integer.getInteger("zookeeper.leaderConnectDelayDuringRetryMs", 100);

    private static final boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");

    public static final String LEARNER_ASYNC_SENDING = "zookeeper.learner.asyncSending";
    private static boolean asyncSending =
            Boolean.parseBoolean(ConfigUtils.getPropertyBackwardCompatibleWay(LEARNER_ASYNC_SENDING));
    static {
        LOG.info("leaderConnectDelayDuringRetryMs: {}", leaderConnectDelayDuringRetryMs);
        LOG.info("TCP NoDelay set to: {}", nodelay);
        LOG.info("{} = {}", LEARNER_ASYNC_SENDING, asyncSending);
    }

    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations = new ConcurrentHashMap<Long, ServerCnxn>();

    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }

    // for testing
    protected static void setAsyncSending(boolean newMode) {
        asyncSending = newMode;
        LOG.info("{} = {}", LEARNER_ASYNC_SENDING, asyncSending);

    }
    protected static boolean getAsyncSending() {
        return asyncSending;
    }
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout) throws IOException {
        LOG.info("Revalidating client: 0x{}", Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos.toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                    LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "To validate session 0x" + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }

    /**
     * write a packet to the leader.
     * 写入一个数据包到 Leader.
     *
     * This method is called by multiple threads. We need to make sure that only one thread is writing to leaderOs at a time.
     * When packets are sent synchronously, writing is done within a synchronization block.
     * When packets are sent asynchronously, sender.queuePacket() is called, which writes to a BlockingQueue, which is thread-safe.
     * Reading from this BlockingQueue and writing to leaderOs is the learner sender thread only.
     * So we have only one thread writing to leaderOs at a time in either case.
     *
     * 该方法由多个线程调用. 我们需要确保一次只向 Leader 写入一个线程.
     * 当数据包被同步发送时, 在 synchronization block 同步块内完成写入.
     * 异步发送数据包时, 将调用 sender.queuePacket() 并将其写入线程安全的 BlockingQueue.
     * 从 BlockingQueue 读取并写入 leaderOs 仅仅是 Leader 发送者的线程.
     * 因此, 在任何一种情况下, 我们一次只能一个线程写入到 leaderOs.
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    // 发送 QuorumPacket 数据包到 Leader
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        if (asyncSending) {
            sender.queuePacket(pp);
        } else {
            writePacketNow(pp, flush);
        }
    }

    void writePacketNow(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                messageTracker.trackSent(pp.getType());
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the leader
     */
    protected void startSendingThread() {
        sender = new LearnerSender(this);
        sender.start();
    }

    /**
     * read a packet from the leader
     * 读取 Leader 的数据包
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet");
            messageTracker.trackReceived(pp.getType());
        }
        if (LOG.isTraceEnabled()) {
            final long traceMask =
                    (pp.getType() == Leader.PING) ? ZooTrace.SERVER_PING_TRACE_MASK
                            : ZooTrace.SERVER_PACKET_TRACE_MASK;

            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }

    /**
     * send a request packet to the leader
     * 向 Leader 发送 Request 请求数据包
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    // 向 Leader 发送 QuorumPacket 请求数据包 (REQUEST)
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte[] b = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos.toByteArray(), request.authInfo);
        writePacket(qp, true);
    }

    /**
     * Returns the address of the node we think is the leader.
     * 返回我们认为是 Leader 节点的地址.
     */
    // 找到 Leader
    protected QuorumServer findLeader() {
        // 通过当前投票的 Leader id, 找出对应的 QuorumServer
        QuorumServer leaderServer = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = {}", current.getId());
        }
        return leaderServer;
    }

    /**
     * Overridable helper method to return the System.nanoTime().
     * This method behaves identical to System.nanoTime().
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Overridable helper method to simply call sock.connect(). This can be
     * overriden in tests to fake connection success/failure for connectToLeader.
     */
    protected void sockConnect(Socket sock, InetSocketAddress addr, int timeout) throws IOException {
        sock.connect(addr, timeout);
    }

    /**
     * Establish a connection with the LearnerMaster found by findLearnerMaster.
     * Followers only connect to Leaders, Observers can connect to any active LearnerMaster.
     * Retries until either initLimit time has elapsed or 5 tries have happened.
     * 与 findLearnerMaster 找到的 LearnerMaster 建立连接.
     * Followers 仅连接到 Leaders, Observers 可以连接到任何活动的 LearnerMaster.
     * 重试直到 initLimit 时间过去或尝试 5 次为止.
     *
     * @param multiAddr - the address of the Peer to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * if there is an authentication failure while connecting to leader
     */
    // 连接 Leader
    protected void connectToLeader(MultipleAddresses multiAddr, String hostname) throws IOException {

        this.leaderAddr = multiAddr;
        Set<InetSocketAddress> addresses;
        if (self.isMultiAddressReachabilityCheckEnabled()) {
            // even if none of the addresses are reachable, we want to try to establish connection
            // see ZOOKEEPER-3758
            addresses = multiAddr.getAllReachableAddressesOrAll();
        } else {
            addresses = multiAddr.getAllAddresses();
        }
        ExecutorService executor = Executors.newFixedThreadPool(addresses.size());
        CountDownLatch latch = new CountDownLatch(addresses.size());
        AtomicReference<Socket> socket = new AtomicReference<>(null);
        // 与 Leader 建立连接 (重试直到 initLimit 时间过去或尝试 5 次为止)
        addresses.stream().map(address -> new LeaderConnector(address, socket, latch)).forEach(executor::submit);

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while trying to connect to Leader", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOG.error("not all the LeaderConnector terminated properly");
                }
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while terminating LeaderConnector executor.", ie);
            }
        }

        if (socket.get() == null) {
            throw new IOException("Failed connect to " + multiAddr);
        } else {
            sock = socket.get();
        }

        self.authLearner.authenticate(sock, hostname);

        // 初始化 leaderIs 和 bufferedOutput (连接 Leader 的网络输入输出流)
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
        if (asyncSending) {
            startSendingThread();
        }
    }

    // 与 Leader 建立连接 (重试直到 initLimit 时间过去或尝试 5 次为止)
    class LeaderConnector implements Runnable {

        private AtomicReference<Socket> socket;
        private InetSocketAddress address;
        private CountDownLatch latch;

        LeaderConnector(InetSocketAddress address, AtomicReference<Socket> socket, CountDownLatch latch) {
            this.address = address;
            this.socket = socket;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Thread.currentThread().setName("LeaderConnector-" + address);
                // 与 Leader 建立连接 (重试直到 initLimit 时间过去或尝试 5 次为止)
                Socket sock = connectToLeader();

                if (sock != null && sock.isConnected()) {
                    if (socket.compareAndSet(null, sock)) {
                        LOG.info("Successfully connected to leader, using address: {}", address);
                    } else {
                        LOG.info("Connection to the leader is already established, close the redundant connection");
                        sock.close();
                    }
                }

            } catch (Exception e) {
                LOG.error("Failed connect to {}", address, e);
            } finally {
                latch.countDown();
            }
        }

        // 与 Leader 建立连接 (重试直到 initLimit 时间过去或尝试 5 次为止)
        private Socket connectToLeader() throws IOException, X509Exception, InterruptedException {
            Socket sock = createSocket();

            // leader connection timeout defaults to tickTime * initLimit
            int connectTimeout = self.tickTime * self.initLimit;

            // but if connectToLearnerMasterLimit is specified, use that value to calculate
            // timeout instead of using the initLimit value
            if (self.connectToLearnerMasterLimit > 0) {
                connectTimeout = self.tickTime * self.connectToLearnerMasterLimit;
            }

            int remainingTimeout;
            long startNanoTime = nanoTime();

            for (int tries = 0; tries < 5 && socket.get() == null; tries++) {
                try {
                    // recalculate the init limit time because retries sleep for 1000 milliseconds
                    remainingTimeout = connectTimeout - (int) ((nanoTime() - startNanoTime) / 1_000_000);
                    if (remainingTimeout <= 0) {
                        LOG.error("connectToLeader exceeded on retries.");
                        throw new IOException("connectToLeader exceeded on retries.");
                    }

                    sockConnect(sock, address, Math.min(connectTimeout, remainingTimeout));
                    if (self.isSslQuorum()) {
                        ((SSLSocket) sock).startHandshake();
                    }
                    sock.setTcpNoDelay(nodelay);
                    break;
                } catch (IOException e) {
                    remainingTimeout = connectTimeout - (int) ((nanoTime() - startNanoTime) / 1_000_000);

                    if (remainingTimeout <= leaderConnectDelayDuringRetryMs) {
                        LOG.error(
                                "Unexpected exception, connectToLeader exceeded. tries={}, remaining init limit={}, connecting to {}",
                                tries,
                                remainingTimeout,
                                address,
                                e);
                        throw e;
                    } else if (tries >= 4) {
                        LOG.error(
                                "Unexpected exception, retries exceeded. tries={}, remaining init limit={}, connecting to {}",
                                tries,
                                remainingTimeout,
                                address,
                                e);
                        throw e;
                    } else {
                        LOG.warn(
                                "Unexpected exception, tries={}, remaining init limit={}, connecting to {}",
                                tries,
                                remainingTimeout,
                                address,
                                e);
                        sock = createSocket();
                    }
                }
                Thread.sleep(leaderConnectDelayDuringRetryMs);
            }

            return sock;
        }
    }

    /**
     * Creating a simple or and SSL socket.
     * This can be overridden in tests to fake already connected sockets for connectToLeader.
     */
    protected Socket createSocket() throws X509Exception, IOException {
        Socket sock;
        if (self.isSslQuorum()) {
            sock = self.getX509Util().createSSLSocket();
        } else {
            sock = new Socket();
        }
        sock.setSoTimeout(self.tickTime * self.initLimit);
        return sock;
    }

    /**
     * Once connected to the leader or learner master, perform the handshake
     * protocol to establish a following / observing connection.
     * 一旦连接到 Leader 或 Learner 主机, 执行握手协议以建立 following / observing 连接.
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @return Leader 发送的 zxid 用于同步.
     * @throws IOException
     */
    // 注册 Leader
    protected long registerWithLeader(int pktType) throws IOException {
        /*
         * Send follower info, including last zxid and sid
         * 发送 follower 信息, 包括当前的最大 zxid 和 sid
         */
        // lastLoggedZxid (当前的最大 zxid)
        long lastLoggedZxid = self.getLastLoggedZxid();

        QuorumPacket qp = new QuorumPacket();
        qp.setType(pktType);
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));

        /*
         * Add sid to payload
         */
        // 发送 FOLLOWERINFO, sid, 版本信息等给 Leader
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000, self.getQuorumVerifier().getVersion());
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");
        qp.setData(bsid.toByteArray());
        writePacket(qp, true);

        // 读取 Leader 的返回结果
        readPacket(qp);
        // 通过 Leader 的 zxid 解析的 Leader 的选举轮次
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
        if (qp.getType() == Leader.LEADERINFO) {
            // 如果是 LEADERINFO

            // we are connected to a 1.0 server so accept the new epoch and read the next packet
            leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
            byte[] epochBytes = new byte[4];
            final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
            if (newEpoch > self.getAcceptedEpoch()) {
                // 如果 Leader 的选举轮次大于当前的, 则更新当前的

                wrappedEpochBytes.putInt((int) self.getCurrentEpoch());
                self.setAcceptedEpoch(newEpoch);
            } else if (newEpoch == self.getAcceptedEpoch()) {
                // 如果 Leader 的选举轮次等于当前的, 则不做处理

                // since we have already acked an epoch equal to the leaders, we cannot ack
                // again, but we still need to send our lastZxid to the leader so that we can
                // sync with it if it does assume leadership of the epoch.
                // the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1);
            } else {
                // 如果 Leader 的选举轮次小于当前的, 则抛出异常

                throw new IOException("Leaders epoch, "
                        + newEpoch
                        + " is less than accepted epoch, "
                        + self.getAcceptedEpoch());
            }

            // 发送 ACKEPOCH, lastLoggedZxid (当前的最大 zxid) 给 Leader
            QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
            writePacket(ackNewEpoch, true);

            // 此方法返回通过 Leader 的选举轮次解析的 Leader 的 zxid
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {
            if (newEpoch > self.getAcceptedEpoch()) {
                self.setAcceptedEpoch(newEpoch);
            }
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    }

    /**
     * Finally, synchronize our history with the Leader (if Follower)
     * or the LearnerMaster (if Observer).
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    // 与 Leader 数据同步
    protected void syncWithLeader(long newLeaderZxid) throws Exception {
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        // 通过 Leader 的 zxid 解析 Leader 的选举轮次
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);

        QuorumVerifier newLeaderQV = null;

        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        boolean snapshotNeeded = true;
        boolean syncSnapshot = false;

        // 读取 Leader 的信息
        readPacket(qp);
        Deque<Long> packetsCommitted = new ArrayDeque<>();
        // 保存未提交的事务队列
        Deque<PacketInFlight> packetsNotCommitted = new ArrayDeque<>();
        synchronized (zk) {

            // 判断同步模式, 执行不同的同步逻辑
            if (qp.getType() == Leader.DIFF) {
                // 如果是 DIFF 同步模式, 执行以下步骤:
                // step 1. 设置同步模式
                // step 2. 不需要 SnapShot 同步

                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                // step 1
                self.setSyncMode(QuorumPeer.SyncMode.DIFF);
                // step 2
                snapshotNeeded = false;

            } else if (qp.getType() == Leader.SNAP) {
                // 如果是 SNAP 同步模式, 执行以下步骤:
                // step 1. 设置同步模式
                // step 2. SnapShot 同步: 通过 Leader 的 SnapShot 反序列化当前节点的 ZKDatabase (即将 Leader 的数据写入 Learner 节点的内存中)
                //         a. 清空当前内存数据库 (ZKDatabase)
                //         b. 反序列化 sessions 和 DataTree
                // step 3. 设置当前节点的 zxid 为 Leader 的 zxid

                // step 1
                self.setSyncMode(QuorumPeer.SyncMode.SNAP);

                // step 2
                LOG.info("Getting a snapshot from leader 0x{}", Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // db is clear as part of deserializeSnapshot()
                zk.getZKDatabase().deserializeSnapshot(leaderIs);

                // ZOOKEEPER-2819: overwrite config node content extracted
                // from leader snapshot with local config, to avoid potential
                // inconsistency of config node content during rolling restart.
                if (!self.isReconfigEnabled()) {
                    LOG.debug("Reset config node content from local config after deserialization of snapshot.");
                    zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
                }
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got {}", signature);
                    throw new IOException("Missing signature");
                }

                // step 3
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

                // immediately persist the latest snapshot when there is txn log gap
                syncSnapshot = true;

            } else if (qp.getType() == Leader.TRUNC) {
                // 如果是 TRUNC 同步模式, 执行以下步骤:
                // step 1. 设置同步模式
                // step 2. 截断删除当前节点大于 Leader 的 zxid 的事务记录
                // step 3. 设置当前节点的 zxid 为 Leader 的 zxid

                //we need to truncate the log to the lastzxid of the leader
                // 我们需要将日志截断到 Leader 的 lastzxid

                // step 1
                self.setSyncMode(QuorumPeer.SyncMode.TRUNC);

                // step 2
                LOG.warn("Truncating log to get in sync with the leader 0x{}", Long.toHexString(qp.getZxid()));
                boolean truncated = zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log 0x{}", Long.toHexString(qp.getZxid()));
                    ServiceUtils.requestSystemExit(ExitCode.QUORUM_PACKET_ERROR.getValue());
                }

                // step 3
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

            } else {
                LOG.error("Got unexpected packet from leader: {}, exiting ... ", LearnerHandler.packetToString(qp));
                ServiceUtils.requestSystemExit(ExitCode.QUORUM_PACKET_ERROR.getValue());
            }
            zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
            zk.createSessionTracker();

            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;
            TxnLogEntry logEntry;
            // we are now going to start getting transactions to apply followed by an UPTODATE

            // 如果是 SNAP 同步模式, 会收到 Leader 的 NEWLEADER 和 UPTODATE
            // 如果是 DIFF 同步模式, 会循环收到 Leader 的多个事务 PROPOSAL 和 COMMIT
            outerLoop:
            while (self.isRunning()) {
                // 读取 Leader 的数据
                readPacket(qp);
                switch (qp.getType()) {
                    case Leader.PROPOSAL:
                        // PROPOSAL 表示事务 (DIFF 模式下会收到)
                        // step 1. 读取事务消息头和消息体
                        // step 2. 加入到 packetsNotCommitted (未提交的事务队列中)
                        // step 3. 继续 while() 循环

                        // step 1
                        PacketInFlight pif = new PacketInFlight();
                        logEntry = SerializeUtils.deserializeTxn(qp.getData());
                        pif.hdr = logEntry.getHeader();
                        pif.rec = logEntry.getTxn();
                        pif.digest = logEntry.getDigest();
                        if (pif.hdr.getZxid() != lastQueued + 1) {
                            LOG.warn(
                                    "Got zxid 0x{} expected 0x{}",
                                    Long.toHexString(pif.hdr.getZxid()),
                                    Long.toHexString(lastQueued + 1));
                        }
                        lastQueued = pif.hdr.getZxid();

                        if (pif.hdr.getType() == OpCode.reconfig) {
                            SetDataTxn setDataTxn = (SetDataTxn) pif.rec;
                            QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
                            self.setLastSeenQuorumVerifier(qv, true);
                        }

                        // step 2
                        packetsNotCommitted.add(pif);
                        break;
                    case Leader.COMMIT:
                    case Leader.COMMITANDACTIVATE:
                        // COMMIT 表示提交事务 (DIFF 模式下会收到)
                        // step 1. 从 packetsNotCommitted (未提交的事务队列中) 中获取一个事务
                        // step 2. 执行事务
                        // step 3. 从 packetsNotCommitted (未提交的事务队列中) 中删除该事务
                        // step 4. 继续 while() 循环

                        // step 1
                        pif = packetsNotCommitted.peekFirst();

                        if (pif.hdr.getZxid() == qp.getZxid() && qp.getType() == Leader.COMMITANDACTIVATE) {
                            QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) pif.rec).getData()));
                            boolean majorChange = self.processReconfig(
                                    qv,
                                    ByteBuffer.wrap(qp.getData()).getLong(), qp.getZxid(),
                                    true);
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        }
                        if (!writeToTxnLog) {
                            if (pif.hdr.getZxid() != qp.getZxid()) {
                                LOG.warn(
                                        "Committing 0x{}, but next proposal is 0x{}",
                                        Long.toHexString(qp.getZxid()),
                                        Long.toHexString(pif.hdr.getZxid()));
                            } else {
                                // step 2
                                zk.processTxn(pif.hdr, pif.rec);
                                // step 3
                                packetsNotCommitted.remove();
                            }
                        } else {
                            packetsCommitted.add(qp.getZxid());
                        }
                        break;
                    case Leader.INFORM:
                    case Leader.INFORMANDACTIVATE:
                        PacketInFlight packet = new PacketInFlight();

                        if (qp.getType() == Leader.INFORMANDACTIVATE) {
                            ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                            long suggestedLeaderId = buffer.getLong();
                            byte[] remainingdata = new byte[buffer.remaining()];
                            buffer.get(remainingdata);
                            logEntry = SerializeUtils.deserializeTxn(remainingdata);
                            packet.hdr = logEntry.getHeader();
                            packet.rec = logEntry.getTxn();
                            packet.digest = logEntry.getDigest();
                            QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) packet.rec).getData()));
                            boolean majorChange = self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        } else {
                            logEntry = SerializeUtils.deserializeTxn(qp.getData());
                            packet.rec = logEntry.getTxn();
                            packet.hdr = logEntry.getHeader();
                            packet.digest = logEntry.getDigest();
                            // Log warning message if txn comes out-of-order
                            if (packet.hdr.getZxid() != lastQueued + 1) {
                                LOG.warn(
                                        "Got zxid 0x{} expected 0x{}",
                                        Long.toHexString(packet.hdr.getZxid()),
                                        Long.toHexString(lastQueued + 1));
                            }
                            lastQueued = packet.hdr.getZxid();
                        }
                        if (!writeToTxnLog) {
                            // Apply to db directly if we haven't taken the snapshot
                            zk.processTxn(packet.hdr, packet.rec);
                        } else {
                            packetsNotCommitted.add(packet);
                            packetsCommitted.add(qp.getZxid());
                        }

                        break;
                    case Leader.UPTODATE:
                        // UPTODATE 表示 (SNAP 模式下会收到)
                        // step 1. 设置 ZooKeeperServer
                        // step 2. 跳出 while() 循环

                        LOG.info("Learner received UPTODATE message");
                        if (newLeaderQV != null) {
                            boolean majorChange = self.processReconfig(newLeaderQV, null, null, true);
                            if (majorChange) {
                                throw new Exception("changes proposed in reconfig");
                            }
                        }
                        // 兼容标志, NEWLEADER 中已经设置为 false
                        if (isPreZAB1_0) {
                            zk.takeSnapshot(syncSnapshot);
                            self.setCurrentEpoch(newEpoch);
                        }
                        // step 1
                        self.setZooKeeperServer(zk);
                        self.adminServer.setZooKeeperServer(zk);
                        // step 2
                        break outerLoop;
                    case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery
                        // NEWLEADER 表示已经完成和 Leader 的 SnapShot 的同步 (SNAP 模式下会收到)
                        // step 1. 如果是非 DIFF 同步模式, 则生成 SnapShot 文件
                        // step 2. 更新选举轮次
                        // step 3. 发送 ACK 给 Leader
                        // step 4. 继续 while() 循环

                        // means this is Zab 1.0
                        LOG.info("Learner received NEWLEADER message");
                        if (qp.getData() != null && qp.getData().length > 1) {
                            try {
                                QuorumVerifier qv = self.configFromString(new String(qp.getData()));
                                self.setLastSeenQuorumVerifier(qv, true);
                                newLeaderQV = qv;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // step 1
                        if (snapshotNeeded) {
                            zk.takeSnapshot(syncSnapshot);
                        }

                        // step 2
                        self.setCurrentEpoch(newEpoch);
                        writeToTxnLog = true;
                        //Anything after this needs to go to the transaction log, not applied directly in memory
                        // 兼容标志
                        isPreZAB1_0 = false;

                        // ZOOKEEPER-3911: make sure sync the uncommitted logs before commit them (ACK NEWLEADER).
                        sock.setSoTimeout(self.tickTime * self.syncLimit);
                        self.setSyncMode(QuorumPeer.SyncMode.NONE);
                        zk.startupWithoutServing();
                        if (zk instanceof FollowerZooKeeperServer) {
                            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer) zk;
                            for (PacketInFlight p : packetsNotCommitted) {
                                fzk.logRequest(p.hdr, p.rec, p.digest);
                            }
                            packetsNotCommitted.clear();
                        }

                        // step 3
                        writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                        break;
                }
            }
        }
        // 最后发送 ACK 给 Leader 表示同步完成
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true);

        // 启动 ZooKeeperServer
        zk.startServing();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         *
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec, p.digest);
            }
            for (Long zxid : packetsCommitted) {
                fzk.commit(zxid);
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn(
                            "Committing 0x{}, but next proposal is 0x{}",
                            Long.toHexString(zxid),
                            Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(), p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.setTxn(p.rec);
                request.setHdr(p.hdr);
                request.setTxnDigest(p.digest);
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }

    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations.remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x{} for validation", Long.toHexString(sessionId));
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                    LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId) + " is valid: " + valid);
        }
    }

    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Map<Long, Integer> touchTable = zk.getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }

        QuorumPacket pingReply = new QuorumPacket(qp.getType(), qp.getZxid(), bos.toByteArray(), qp.getAuthinfo());
        writePacket(pingReply, true);
    }

    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        self.setZooKeeperServer(null);
        self.closeAllConnections();
        self.adminServer.setZooKeeperServer(null);

        if (sender != null) {
            sender.shutdown();
        }

        closeSocket();
        // shutdown previous zookeeper
        if (zk != null) {
            // If we haven't finished SNAP sync, force fully shutdown
            // to avoid potential inconsistency
            zk.shutdown(self.getSyncMode().equals(QuorumPeer.SyncMode.SNAP));
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    void closeSocket() {
        try {
            if (sock != null) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring error closing connection to leader", e);
        }
    }

}
