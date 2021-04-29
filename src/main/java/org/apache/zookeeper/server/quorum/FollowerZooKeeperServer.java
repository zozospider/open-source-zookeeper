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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.management.JMException;
import org.apache.jute.Record;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -&gt; CommitProcessor -&gt;
 * FinalRequestProcessor
 *
 * 就像标准的 ZooKeeperServer一样. 我们只是替换了 request processors 请求处理器:
 * FollowerRequestProcessor -> CommitProcessor -> FinalRequestProcessor
 *
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 * 还产生了一个 SyncRequestProcessor 来记录 Leader 的 Proposals.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs;

    /**
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower() {
        return self.follower;
    }

    // 请求处理链:
    // FollowerRequestProcessor -> CommitProcessor -> FinalRequestProcessor
    // SyncRequestProcessor -> SendAckRequestProcessor
    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor(getFollower()));
        syncProcessor.start();
    }

    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    // 处理 Request 消息, 将消息传入请求处理链 SyncRequestProcessor
    public void logRequest(TxnHeader hdr, Record txn, TxnDigest digest) {
        Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        request.setTxnDigest(digest);
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        // 将 Request 传入请求处理链 SyncRequestProcessor
        // 完整处理链为: SyncRequestProcessor -> SendAckRequestProcessor
        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called,
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * 收到 COMMIT 消息后, 最终将调用此方法, 该方法将 COMMIT 中的 zxid 与 (希望) 未决 Txns 队列的头匹配,
     * 并将其交给 commitProcessor 进行提交.
     *
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    // 处理 Request 消息, 将消息传入请求处理链 CommitProcessor
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid) + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                    + " but next pending txn 0x" + Long.toHexString(firstElementZxid));
            ServiceUtils.requestSystemExit(ExitCode.UNMATCHED_TXN_COMMIT.getValue());
        }
        Request request = pendingTxns.remove();
        request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);

        // 将 Request 传入请求处理链 CommitProcessor
        // 完整处理链为: CommitProcessor -> FinalRequestProcessor
        commitProcessor.commit(request);
    }

    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        if (r instanceof LearnerSyncRequest) {
            LearnerSyncRequest lsr = (LearnerSyncRequest) r;
            lsr.fh.queuePacket(new QuorumPacket(Leader.SYNC, 0, null, null));
        }
        commitProcessor.commit(r);
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        return globalOutstandingLimit;
    }

    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }

    /**
     * Process a request received from external Learner through the LearnerMaster
     * These requests have already passed through validation and checks for
     * session upgrade and can be injected into the middle of the pipeline.
     *
     * @param request received from external Learner
     */
    void processObserverRequest(Request request) {
        ((FollowerRequestProcessor) firstProcessor).processRequest(request, false);
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        try {
            MBeanRegistry.getInstance().register(handlerBean, jmxServerBean);
            return true;
        } catch (JMException e) {
            LOG.warn("Could not register connection", e);
        }
        return false;
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("synced_observers", self::getSynced_observers_metric);

    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();
        rootContext.unregisterGauge("synced_observers");

    }

}
