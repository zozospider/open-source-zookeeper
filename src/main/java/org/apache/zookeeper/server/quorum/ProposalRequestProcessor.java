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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 * 该 RequestProcessor 只是将请求转发到 AckRequestProcessor 和 SyncRequestProcessor.
 */
public class ProposalRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    // 请求处理链:
    // SyncRequestProcessor -> AckRequestProcessor
    public ProposalRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    // 处理请求
    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */

        if (request instanceof LearnerSyncRequest) {
            zks.getLeader().processSync((LearnerSyncRequest) request);
        } else {
            // 处理请求链中的下一个
            // nextProcessor 的实现为 CommitProcessor
            nextProcessor.processRequest(request);

            if (request.getHdr() != null) {
                // 如果是事务请求, 则通过 Leader 发送 proposal 给其他 Learners
                // 然后处理请求链中的下一个: SyncRequestProcessor

                // We need to sync and get consensus on any transactions
                // 我们需要对任何事务进行同步并达成共识
                try {
                    // Leader 发送 Proposal 给其他 Learners
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }

                // 处理请求链中的下一个: SyncRequestProcessor
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
