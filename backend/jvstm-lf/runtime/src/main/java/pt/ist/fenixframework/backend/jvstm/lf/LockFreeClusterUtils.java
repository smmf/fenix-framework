/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.lf;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.pstm.CommitOnlyTransaction;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

public class LockFreeClusterUtils {

    private static final Logger logger = LoggerFactory.getLogger(LockFreeClusterUtils.class);
//    private static final String FF_GLOBAL_LOCK_NAME = "ff.hzl.global.lock";
//    private static final String FF_GLOBAL_LOCK_NUMBER_NAME = "ff.hzl.global.lock.number";
//    private static final long FF_GLOBAL_LOCK_LOCKED_VALUE = -1;
    public static final String FF_COMMIT_TOPIC_NAME = "ff.hzl.commits";

    private static HazelcastInstance HAZELCAST_INSTANCE;

    /**
     * A buffer for the commit requests.
     */
    private static CommitRequestsBuffer requestsBuffer;// = new CommitRequestsBuffer();

    // commit requests that have not been applied yet
    private static final AtomicReference<CommitRequest> commitRequestsHead = new AtomicReference<CommitRequest>();
    // this avoids iterating from the head every time a commit request arrives.  Is only used by the (single) thread that enqueues requests
    private static CommitRequest commitRequestsTail = null;

//    // where to append commit requests. may be outdated due to concurrency, so we need to be careful when updating this reference 
//    private static volatile AtomicReference<CommitRequest> commitRequestTail = new AtomicReference<CommitRequest>(null);

//    private static final ConcurrentLinkedQueue<CommitRequest> COMMIT_REQUESTS = new ConcurrentLinkedQueue<CommitRequest>();

    static class CommitRequestComparator implements Comparator<CommitRequest> {
        @Override
        public int compare(CommitRequest cr1, CommitRequest cr2) {
            if (cr1.getSendCount() == cr2.getSendCount()) {
                return cr1.getId().compareTo(cr2.getId());
            } else {
                return cr2.getSendCount() - cr1.getSendCount();
            }
        }
    }

    private LockFreeClusterUtils() {
    }

    public static void initializeGroupCommunication(JvstmLockFreeConfig thisConfig) {
        commitRequestsHead.set(CommitRequest.makeSentinelRequest());
        commitRequestsTail = getCommitRequestAtHead();

        com.hazelcast.config.Config hzlCfg = thisConfig.getHazelcastConfig();
        HAZELCAST_INSTANCE = Hazelcast.newHazelcastInstance(hzlCfg);

        LockFreeClusterUtils.requestsBuffer = new CommitRequestsBuffer(thisConfig.getExpectedInitialNodes() * 4);

        // register listener for commit requests
        registerListenerForCommitRequests();

        /* some repositories may not implement correctly the initial state
        transfer mechanism.  So, we wait until the initial number of nodes is
        up before continuing with the initialization.  This allows us to work
        with "initially-stateless" repositories in addition to the 'normal'
        state-transfer-enabled ones. */

        int expectedInitialNodes = thisConfig.getExpectedInitialNodes();
        if (expectedInitialNodes > 1) {
            logger.info("Waiting for the initial number of nodes: {}", expectedInitialNodes);

            waitForMembers("backend-jvstm-cluster-init-barrier", expectedInitialNodes);

            logger.info("All initial nodes are available");
        }
    }

//    private static final ReentrantLock ENQUEUE_LOCK = new ReentrantLock(true);

    private static void registerListenerForCommitRequests() {
        ITopic<CommitRequest> topic = getHazelcastInstance().getTopic(FF_COMMIT_TOPIC_NAME);

        topic.addMessageListener(new MessageListener<CommitRequest>() {

            @Override
            public final void onMessage(Message<CommitRequest> message) {
                CommitRequest commitRequest = message.getMessageObject();

                logger.debug("Received commit request message. id={}, serverId={}", commitRequest.getId(),
                        commitRequest.getServerId());

                // check for SYNC
                if (commitRequest.getId().equals(CommitRequest.SYNC_REQUEST.getId())) {
                    requestsBuffer.release();
                    return;
                } else {
                    commitRequest.assignTransaction();
                    requestsBuffer.enqueue(commitRequest);
                }

//                enqueueCommitRequest(commitRequest);
            }

//            private final void enqueueCommitRequest(CommitRequest commitRequest) {
//                CommitRequest last = commitRequestsTail;
//
//                // according to Hazelcast, onMessage() runs on a single thread, so this CAS should never fail
//                if (!last.setNext(commitRequest)) {
//                    enqueueFailed();
//                }
//                // update last known tail
//                commitRequestsTail = commitRequest;
//            }
//
//            private void enqueueFailed() throws AssertionError {
//                String message = "Impossible condition: failed to enqueue commit request";
//                logger.error(message);
//                throw new AssertionError(message);
//            }

        });
    }

//    public static void initGlobalCommittedNumber(int value) {
//        AtomicNumber lockNumber = getHazelcastInstance().getAtomicNumber(FF_GLOBAL_LOCK_NUMBER_NAME);
//        lockNumber.compareAndSet(0, value);
//    }

    // the instance should have been initialized in a single thread within the
    // FenixFramework static initializer's lock (via the invocation of the method
    // initializeGroupCommunication.
    private static HazelcastInstance getHazelcastInstance() {
        return HAZELCAST_INSTANCE;
    }

    public static void notifyStartupComplete() {
        logger.info("Notify other nodes that startup completed");

        IAtomicLong initMarker = getHazelcastInstance().getAtomicLong("initMarker");
        initMarker.incrementAndGet();
    }

    public static void waitForStartupFromFirstNode() {
        logger.info("Waiting for startup from first node");

        // check initMarker in AtomicNumber (value 1)
        IAtomicLong initMarker = getHazelcastInstance().getAtomicLong("initMarker");

        while (initMarker.get() == 0) {
            logger.debug("Waiting for first node to startup...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        logger.debug("First node startup is complete.  We can proceed.");
    }

    public static int obtainNewServerId() {
        /* currently does not reuse the server Id value while any server is up.
        This can be changed if needed.  However, we currently depend on the first
        server getting the AtomicNumber 0 to know that it is the first member
        to appear.  By reusing numbers with the cluster alive, we either don't
        reuse 0 or change the algorithm  that detects the first member */

        IAtomicLong serverIdGenerator = getHazelcastInstance().getAtomicLong("serverId");
        long longId = serverIdGenerator.getAndAdd(1L);

        logger.info("Got (long) serverId: {}", longId);

        int intId = (int) longId;
        if (intId != longId) {
            throw new Error("Failed to obtain a valid id");
        }

        return intId;
    }

    public static void sendCommitRequest(CommitRequest commitRequest) {
        // test for debug, because computing commitRequest.toString() is expensive
        if (logger.isDebugEnabled()) {
            logger.debug("Send commit request to others: {}", commitRequest);
        }

        ITopic<CommitRequest> topic = getHazelcastInstance().getTopic(FF_COMMIT_TOPIC_NAME);
        topic.publish(commitRequest);
    }

    /**
     * Get the first element in the commit requests queue.
     * 
     * @return The {@link CommitRequest} at the head of the queue.
     */
    public static CommitRequest getCommitRequestAtHead() {
        return commitRequestsHead.get();
    }

    public static CommitRequest getCommitRequestsTail() {
        return commitRequestsTail;
    }

    /**
     * Clears the given commit request from the head of the remote commits queue if: (1) there is a next one; AND (2) the head is
     * still the given request. This method should only be invoked when the commit request to remove is already handled (either
     * committed or marked as undecided).
     * 
     * FOR CODE SAFETY, this method cannot skip requests. If needed, it must iterate until if finds the request it wants to
     * return.
     * 
     * @param commitRequest The commitRequest to remove from head
     * @return The commit request left at the head. This can be either: (1) The commit request given as argument (if it could not
     *         be removed); (2) the commit request following the one given in the argument; or (3) any other commit request (if
     *         the one given as argument was no longer at the head (which means it had been removed already by another thread.
     */
    public static CommitRequest tryToRemoveCommitRequest(CommitRequest commitRequest) {
        CommitRequest next = commitRequest.getNext();

        if (next == null) {
            logger.debug("Commit request {} has no next yet.  Must remain at the head", commitRequest.getId());
            return commitRequest;
        }

        if (commitRequestsHead.compareAndSet(commitRequest, next)) {
            logger.debug("Removed commit request {} from the head.", commitRequest.getId());
//            return next;
        } else {
            logger.debug("Commit request {} was no longer at the head.", commitRequest.getId());
//            return commitRequestsHead.get();
        }
        return next;

    }

    public static void shutdown() {
        HazelcastInstance hzl = getHazelcastInstance();
        hzl.getTopic(FF_COMMIT_TOPIC_NAME).destroy();
        hzl.getLifecycleService().shutdown();

        /* strangely this is here.  Perhaps we should move these clear()s
        elsewhere. They are needed when the classes are reused via
        FenixFramework.shutdown()/initialize().  I guess these maps should be
        in this class not in COTx */
        CommitOnlyTransaction.txVersionToCommitIdMap.clear();
        CommitOnlyTransaction.commitsMap.clear();
    }

    /**
     * Get the number of members in the cluster.
     * 
     * @return The number of members in the cluster or <code>-1</code> if the information is not available
     */
    public static int getNumMembers() {
        if (!getHazelcastInstance().getLifecycleService().isRunning()) {
            return -1;
        } else {
            return getHazelcastInstance().getCluster().getMembers().size();
        }

    }

    /* This is intended as a replacement for JGroup's-based implementation of
    Config.waitForExpectedInitialNodes(). */
    public static void waitForMembers(String barrierName, int members) {
        while (true) {
            logger.info("Waiting at the barrier for {} member(s)", members);
            try {
                boolean success = barrier(barrierName, members);
                if (success) {
                    logger.info("All {} member(s) have awaited at the barrier {}", members, barrierName);
                    return;
                } else {
                    logger.info("Await at {} timedout. Will awaiting again...", barrierName);
                }
            } catch (InterruptedException e) {
                logger.info("Await at {} was interrupted. Will awaiting again. Exception: {}", barrierName, e);
            }
        }
    }

    private static synchronized ICountDownLatch getCountDownLatch(String latchName) {
        HazelcastInstance hzl = getHazelcastInstance();

        if (hzl == null) {
            String msg = "No Hazelcast instance available!";
            logger.error(msg);
            throw new Error(msg);
        }

        return hzl.getCountDownLatch(latchName);
    }

    public static boolean barrier(String barrierName, int awaitSize) throws InterruptedException {
        ICountDownLatch barrier = getCountDownLatch(barrierName);

        boolean reset = barrier.trySetCount(awaitSize);
        if (reset) {
            logger.debug("Reset barrier: {} to: {}", barrierName, awaitSize);
        } else {
            logger.debug("Barrier: {} was already set by another member", barrierName);
        }
        logger.debug("Awaiting at barrier: {}", barrierName);

        barrier.countDown();
        return barrier.await(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    static class CommitRequestsBuffer {
        /**
         * A priority queue to buffer commit requests
         */
        private final PriorityQueue<CommitRequest> incommingRequests;
        private int maxBufferSize = 1;

        CommitRequestsBuffer(int initialBufferSize) {
            this.incommingRequests = new PriorityQueue<>(1, new CommitRequestComparator());
            this.maxBufferSize = initialBufferSize;
        }

        void increaseBuffer() {
            this.maxBufferSize <<= 1;
        }

        void decreaseBuffer() {
            if (this.maxBufferSize > 1) {
                int newSize = this.maxBufferSize >> 1;
                this.maxBufferSize = (newSize == 0 ? 1 : newSize);
                checkFull();
            }
        }

        void enqueue(CommitRequest commitRequest) {
            this.incommingRequests.add(commitRequest);
            checkFull();
        }

        void checkFull() {
            if (this.incommingRequests.size() >= this.maxBufferSize) {
                release();
            }
        }

        public void release() {
            CommitRequest first = this.incommingRequests.peek();

            if (first == null) {
                return;  // nothing to do
            }

            CommitRequest current = this.incommingRequests.poll();
            CommitRequest next;

            while ((next = this.incommingRequests.poll()) != null) {
                // this should be running on a single thread, so this CAS should never fail
                if (!current.setNext(next)) {
                    enqueueFailed();
                }
                current = next;
            }

            addToPublicQueue(first, current);
        }

        private void enqueueFailed() throws AssertionError {
            String message = "Impossible condition: failed to enqueue commit request";
            logger.error(message);
            throw new AssertionError(message);
        }

        private void addToPublicQueue(CommitRequest first, CommitRequest last) {
            if (logger.isDebugEnabled()) {
                StringBuilder str = new StringBuilder();

                str.append("Adding to public commit requests queue:");

                CommitRequest current = first;
                do {
                    str.append(" CR={id=");
                    str.append(current.getId());
                    str.append("}, sendCount=");
                    str.append(current.getSendCount());
                    str.append("}");
                } while ((current = current.getNext()) != null);
                logger.debug(str.toString());
            }

            CommitRequest currentTail = LockFreeClusterUtils.commitRequestsTail;

            // this should be running on a single thread, so this CAS should never fail
            if (!currentTail.setNext(first)) {
                enqueueFailed();
            }

            // update last known tail
            LockFreeClusterUtils.commitRequestsTail = last;
        }
    }
}
