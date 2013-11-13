/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.lf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest.BatchCommitRequest;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest.ValidationStatus;
import pt.ist.fenixframework.backend.jvstm.pstm.CommitOnlyTransaction;
import pt.ist.fenixframework.util.FenixFrameworkThread;

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

    private static final LinkedBlockingQueue<CommitRequest> sendBuffer = new LinkedBlockingQueue<>();
//    private static final long SEND_BUFFER_TIME_LIMIT = Long.MAX_VALUE;
//    private static final long SEND_BUFFER_SIZE_LIMIT = 1;

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

        // start the thread that sends commit requests
        new SendBufferThread().start();

//        LockFreeClusterUtils.requestsBuffer = new CommitRequestsBuffer(thisConfig.getExpectedInitialNodes() * 4);
        LockFreeClusterUtils.requestsBuffer = new CommitRequestsBuffer(getCommitRequestAtHead());

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

                if (commitRequest instanceof BatchCommitRequest) {
                    deliverBatch(((BatchCommitRequest) commitRequest).getRequests());
                } else {
                    deliverSingle(commitRequest);
                }
            }

            public final void deliverBatch(CommitRequest[] commitRequests) {
                for (CommitRequest commitRequest : commitRequests) {
                    deliverSingle(commitRequest);
                }
            }

            public final void deliverSingle(CommitRequest commitRequest) {
                logger.debug("Received commit request message. id={}, serverId={}", commitRequest.getIdWithCount(),
                        commitRequest.getServerId());

                // check for SYNC
                if (commitRequest.getId().equals(CommitRequest.SYNC_REQUEST.getId())) {
                    logger.debug("SYNC received. Don't enqueue it. Just flush and reset.");
                    requestsBuffer.flushAndResetBufferSize();
                } else {
                    // replace the map with this request;
                    CommitRequest.storeCommitRequest(commitRequest);

                    commitRequest.assignTransaction();
                    requestsBuffer.enqueue(commitRequest);
                }
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
        commitRequest.setTimestamp(System.nanoTime());
        LockFreeClusterUtils.sendBuffer.offer(commitRequest);
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
            logger.debug("Commit request {} has no next yet.  Must remain at the head", commitRequest.getIdWithCount());
            return commitRequest;
        }

        if (commitRequestsHead.compareAndSet(commitRequest, next)) {
            logger.debug("Removed commit request {} from the head.", commitRequest.getIdWithCount());
//            return next;
        } else {
            logger.debug("Commit request {} was no longer at the head.", commitRequest.getIdWithCount());
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

    // A buffer for the INCOMING commit requests
    static class CommitRequestsBuffer {
        private static final long MAX_WAIT_TIME_NANOS = 100_000L;
        /**
         * Counter for the number of pending UNDECIDED requests. Must be a deterministic value in every node. It is used to
         * determine
         * the buffer size for the incoming requests.
         */
        private static int undecided = 0;
        /**
         * A priority queue to buffer commit requests
         */
        private CommitRequest lastRequestAnalyzed;
        private final PriorityQueue<CommitRequest> incommingRequests;
        private int maxBufferSize = 1;

        long maxTimestamp = 0L;
        long minTimestamp = Long.MAX_VALUE;

//        private int lastMaxSendCount = 1; // everything is sent at least once

//        CommitRequestsBuffer(int initialBufferSize) {
//            this.incommingRequests = new PriorityQueue<>(1, new CommitRequestComparator());
//            this.maxBufferSize = initialBufferSize;
//        }

        CommitRequestsBuffer(CommitRequest initial) {
            this.lastRequestAnalyzed = initial;
            this.incommingRequests = new PriorityQueue<>(1, new CommitRequestComparator());
            this.maxBufferSize = 1;
        }

        private void resetTimestamps() {
            this.maxTimestamp = 0L;
            this.minTimestamp = Long.MAX_VALUE;
        }

//        void defaultIncreaseBuffer() {
//            this.maxBufferSize <<= 1;
//        }

//        void defaultDecreaseBuffer() {
//            if (this.maxBufferSize > 1) {
//                int newSize = this.maxBufferSize >> 1;
//                this.maxBufferSize = (newSize == 0 ? 1 : newSize);
//                checkFull();
//            }
//        }

//        void deltaIncreaseBuffer(int delta) {
//            this.maxBufferSize += delta;
//        }

//        void deltaDecreaseBuffer(int delta) {
//            int newSize = this.maxBufferSize - delta;
//            this.maxBufferSize = (newSize < 1 ? 1 : newSize);
//            checkFull();
//        }

        private void setMaxSize(int value) {
            logger.debug("Adjusting commit requests' buffer size: {}", value);
            this.maxBufferSize = value;
            checkFull();
        }

//        int getMaxSize() {
//            return this.maxBufferSize;
//        }

        boolean isEmpty() {
            return this.incommingRequests.isEmpty();
        }

        void enqueue(CommitRequest commitRequest) {
            updateTimestamp(commitRequest);
            this.incommingRequests.add(commitRequest);

            if (commitRequest.getReset()) {
                flushAndResetBufferSize();
            } else {
                checkFull();
            }
        }

        private boolean waitingForTooLong() {
            return (this.maxTimestamp - this.minTimestamp) >= MAX_WAIT_TIME_NANOS;
        }

        private void updateTimestamp(CommitRequest commitRequest) {
            long ts = commitRequest.getTimestamp();
            if (ts > this.maxTimestamp) {
                this.maxTimestamp = ts;
            }
            if (ts < this.minTimestamp) {
                this.minTimestamp = ts;
            }
        }

        private void checkFull() {
            if (((!this.incommingRequests.isEmpty()) && (this.incommingRequests.size() >= this.maxBufferSize))
                    || waitingForTooLong()) {
                flushAndUpdateBufferSize();
            }
        }

        private void updateUndecided(CommitRequest upToThisRequest) {
            logger.debug("updateUndecided() up to {}.  LastRequestAnalyzed={}",
                    (upToThisRequest == null ? "NULL" : upToThisRequest.getIdWithCount()),
                    this.lastRequestAnalyzed.getIdWithCount());

            CommitRequest currentRequest = this.lastRequestAnalyzed.getNext();

            while (currentRequest != upToThisRequest) {
                logger.debug("currentRequest={}", (currentRequest == null ? "NULL" : currentRequest.getIdWithCount()));
                ValidationStatus validationStatus = waitForValidationStatus(currentRequest);
                if (validationStatus == ValidationStatus.UNDECIDED && currentRequest.getSendCount() == 1) { // another undecided record
                    CommitRequestsBuffer.undecided++;
                } else if (validationStatus == ValidationStatus.VALID && currentRequest.getSendCount() > 1) { // a previously undecided just got valid
                    int newValue = CommitRequestsBuffer.undecided - 1;
                    CommitRequestsBuffer.undecided = (newValue < 0 ? 0 : newValue);
                }
                this.lastRequestAnalyzed = currentRequest;
                currentRequest = currentRequest.getNext();
            }
            logger.debug("END updateUndecided(). lastRequestAnalyzed={}", this.lastRequestAnalyzed.getIdWithCount());
        }

        private ValidationStatus waitForValidationStatus(CommitRequest currentRequest) {
            ValidationStatus validationStatus;
            while ((validationStatus = currentRequest.getValidationStatus()) == ValidationStatus.UNSET) {
                ;
            }
            logger.debug("waited for validation of request {} (status={})", currentRequest.getIdWithCount(), validationStatus);
            return validationStatus;
        }

        public void flushAndResetBufferSize() {
            logger.debug("flushAndResetBufferSize()");

            flush();

            // reset the undecided count
            this.lastRequestAnalyzed = LockFreeClusterUtils.commitRequestsTail;
            CommitRequestsBuffer.undecided = 0;

            adjustBufferSize();

        }

        public void flushAndUpdateBufferSize() {
            logger.debug("flushAndUpdateBufferSize()");

            // grab a reference to 'first' before flushing (being 'null' is allowed)
            CommitRequest first = this.incommingRequests.peek();

            logger.debug("First request in buffer is {}", first != null ? first.getIdWithCount() : "NULL");

            flush();

            // updates the undecided count up to and excluding 'first'. 'first' may be null, in which case the remainder of the queue is processed
            // must be done after flush to ensure that 'first' is already enqueued and visible (if != null)
            updateUndecided(first);

            adjustBufferSize();
        }

        // no-op if 'first' is null
        private void flush(/*CommitRequest first*/) throws AssertionError {
            CommitRequest first = this.incommingRequests.peek();

            if (first == null) {
                logger.debug("flush() called on empty buffer. skipping...");
                return;
            }

//            long maxTimestamp = first.getTimestamp();
//            long minTimestamp = first.getTimestamp();

            int size = this.incommingRequests.size();

            logger.debug("Flushing commit requests buffer with {} requests. Max size={}", this.incommingRequests.size(),
                    this.maxBufferSize);

            CommitRequest current = this.incommingRequests.poll();
//            int maxSendCount = current.getSendCount();
            CommitRequest next;
            while ((next = this.incommingRequests.poll()) != null) {
                // this should be running on a single thread, so this CAS should never fail
                if (!current.setNext(next)) {
                    enqueueFailed();
                }

//                long ts = next.getTimestamp();
//                if (ts > maxTimestamp) {
//                    maxTimestamp = ts;
//                }
//                if (ts < minTimestamp) {
//                    minTimestamp = ts;
//                }
                current = next;
            }

            logger.debug("Max wait time: {}. bufferSize: {}.  maxSize: {}", this.maxTimestamp - this.minTimestamp, size,
                    this.maxBufferSize);
            resetTimestamps();

            addToPublicQueue(first, current);
        }

        private void adjustBufferSize() {
            setMaxSize(CommitRequestsBuffer.undecided + 1);
        }

//        private void checkMaxSize(int maxSendCount) {
//            // when within [ lastMaxSendCount-1 ; lastMaxSendCount ] keep the current buffer size  
//            if ((maxSendCount < this.lastMaxSendCount - 1) || (maxSendCount > this.lastMaxSendCount)) {
//                logger.debug("Adjusting buffer size from {} to {}.", this.lastMaxSendCount, maxSendCount);
//                setMaxSize(maxSendCount);
//                this.lastMaxSendCount = maxSendCount;
//            }
//        }

        private void enqueueFailed() throws AssertionError {
            String message = "Impossible condition: failed to enqueue commit request";
            logger.error(message);
            throw new AssertionError(message);
        }

        private void addToPublicQueue(CommitRequest first, CommitRequest last) {
            if (logger.isDebugEnabled()) {
                StringBuilder str = new StringBuilder();

                str.append("Adding to public commit requests queue (first=");
                str.append(first.getIdWithCount());
                str.append(", last=");
                str.append(last.getIdWithCount());
                str.append("):");

                CommitRequest current = first;
                do {
                    str.append(" CR={id=");
                    str.append(current.getIdWithCount());
                    str.append(", sendCount=");
                    str.append(current.getSendCount());
                    str.append("}");
                } while ((current = current.getNext()) != null);
                logger.debug(str.toString());
            }

            CommitRequest currentTail = LockFreeClusterUtils.commitRequestsTail;
            logger.debug("currentTail={} (before enqueue)", LockFreeClusterUtils.commitRequestsTail.getIdWithCount());

            // this should be running on a single thread, so this CAS should never fail
            if (!currentTail.setNext(first)) {
                enqueueFailed();
            }

            // update last known tail
            LockFreeClusterUtils.commitRequestsTail = last;
            logger.debug("currentTail={} (after enqueue)", LockFreeClusterUtils.commitRequestsTail.getIdWithCount());
        }
    }

    private static class SendBufferThread extends FenixFrameworkThread {

        protected SendBufferThread() {
            super("FF Commits Sender");
        }

        @Override
        public void run() {
            while (true) {
                // wait for an element
                CommitRequest first;
                try {
                    first = LockFreeClusterUtils.sendBuffer.take();
                } catch (InterruptedException e) {
                    logger.warn("Commit requests sender thread was interrupted.  Terminating...");
                    return;
                }

                flushSendBuffer(first);
            }
        }

//      public static void sendCommitRequestBuffered(CommitRequest commitRequest) {
//      commitRequest.setTimestamp(System.nanoTime());
//      synchronized (LockFreeClusterUtils.class) {
//
//          LockFreeClusterUtils.sendBuffer.offer(commitRequest);
//
//          if (commitRequest.getReset() || commitRequest.getId().equals(CommitRequest.SYNC_REQUEST.getId())
//                  || sendBufferTimedOut(commitRequest.getTimestamp()) || sendBufferAtLimit()) {
//              flushSendBuffer();
//          }
//      }
//  }

//        private boolean sendBufferAtLimit() {
//            return LockFreeClusterUtils.sendBuffer.size() >= LockFreeClusterUtils.SEND_BUFFER_SIZE_LIMIT;
//        }
//
//        private boolean sendBufferTimedOut(long currentTimestamp) {
//            return currentTimestamp - LockFreeClusterUtils.sendBuffer.peek().getTimestamp() > SEND_BUFFER_TIME_LIMIT;
//        }

        private static void flushSendBuffer(CommitRequest first) {
            ArrayList<CommitRequest> commitRequests = new ArrayList<>(100);
            commitRequests.add(first);

            // take as many as there are available
            CommitRequest current;
            while ((current = LockFreeClusterUtils.sendBuffer.poll()) != null) {
                commitRequests.add(current);
            }

            CommitRequest toSend;
            if (commitRequests.size() == 1) { // send single request
                toSend = commitRequests.get(0);
                logger.debug("Sending a single commit request: {}", toSend.getIdWithCount());
            } else { // send a batch of requests
                CommitRequest[] requestsArray = commitRequests.toArray(new CommitRequest[commitRequests.size()]);
                logger.debug("Sending a batch of {} commit requests.", requestsArray.length);

                BatchCommitRequest batch = CommitRequest.makeBatch(requestsArray);

                if (logger.isDebugEnabled()) {
                    for (CommitRequest commitRequest : batch.getRequests()) {
                        CommitRequest previous = CommitRequest.lookup(commitRequest.getId());
                        if (previous != null) {
                            long diff = commitRequest.getTimestamp() - previous.getTimestamp();
                            logger.debug("Round-trip time (ns): {}", diff, commitRequest.getIdWithCount());
                        }
                    }
                }

                toSend = batch;
            }

            ITopic<CommitRequest> topic = getHazelcastInstance().getTopic(FF_COMMIT_TOPIC_NAME);
            topic.publish(toSend);
        }

    }

}
