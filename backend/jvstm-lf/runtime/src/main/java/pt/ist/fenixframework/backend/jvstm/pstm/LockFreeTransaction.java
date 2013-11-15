/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.pstm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jvstm.ActiveTransactionsRecord;
import jvstm.CommitException;
import jvstm.Transaction;
import jvstm.TransactionSignaller;
import jvstm.VBoxBody;
import jvstm.cps.ConsistentTopLevelTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest.DeltaCommitRequest;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest.ValidationStatus;
import pt.ist.fenixframework.backend.jvstm.lf.JvstmLockFreeBackEnd;
import pt.ist.fenixframework.backend.jvstm.lf.LockFreeClusterUtils;
import pt.ist.fenixframework.backend.jvstm.lf.SimpleWriteSet;
import pt.ist.fenixframework.core.WriteOnReadError;

public class LockFreeTransaction extends ConsistentTopLevelTransaction implements StatisticsCapableTransaction,
        CommitRequestListener {

    private static final Logger logger = LoggerFactory.getLogger(LockFreeTransaction.class);

    private static int NUM_READS_THRESHOLD = 10000000;
    private static int NUM_WRITES_THRESHOLD = 100000;

    public static void activeSleep(long nanos) {
        long now = System.nanoTime();
        long requestedTime = now + nanos;

        while (now < requestedTime) {
            now = System.nanoTime();
        }
    }

    private boolean readOnly = false;

    /**
     * Commit requests seen has undecided that may later be sent has benign requests in this transaction's commit request
     */
    protected Map<UUID, CommitRequest> undecidedMaybeBenign = new HashMap<>();

    /**
     * IDs of ALL commit requests that may be committed ahead of this transaction, and that do not invalidate it.
     */
    protected Set<UUID> allTestedCommitRequestIds = new HashSet<>();

    /**
     * The reference to the last request processed in the queue of commit requests
     */
    private CommitRequest lastProcessedRequest;

    // for statistics
    protected int numBoxReads = 0;
    protected int numBoxWrites = 0;

//    /**
//     * The average number of resents in commit requests seen by this transaction.
//     */
//    private final int avgResend = 0;

    /**
     * The request ID for the commit request of this transaction. Assigned by
     */
    protected UUID myRequestId;

    /**
     * The number of elements considered in the avgResend attribute;
     */
    private static final int RESEND_WINDOW = 10;

    private static final long SYNC_TIMEOUT = 1000_000_000L;

    public LockFreeTransaction(ActiveTransactionsRecord record) {
        super(record);

        logger.debug("Initial read version is {}", record.transactionNumber);

        upgradeWithPendingCommitsAtBeginning();
    }

    protected void upgradeWithPendingCommitsAtBeginning() {
        this.lastProcessedRequest = processCommitRequests(LockFreeClusterUtils.getCommitRequestAtHead(), this);

        ActiveTransactionsRecord newRecord = Transaction.mostRecentCommittedRecord;
        logger.debug("Done processing pending commit requests.  Most recent version is {}", newRecord.transactionNumber);

        if (newRecord != this.activeTxRecord) {
            logger.debug("Upgrading read version to {}", newRecord.transactionNumber);
            upgradeTx(newRecord);
        }
    }

//    protected void updateAverageResends(int resendCount) {
//        int thisAvg = this.avgResend;
//        this.avgResend = (this.avgResend * RESEND_WINDOW + resendCount) / (RESEND_WINDOW + 1);
//        logger.debug("Update avg resends. {} -> {}", thisAvg, this.avgResend);
//    }

    @Override
    public void setReadOnly() {
        this.readOnly = true;
    }

    @Override
    public int getNumBoxReads() {
        return numBoxReads;
    }

    @Override
    public int getNumBoxWrites() {
        return numBoxWrites;
    }

    @Override
    public boolean txAllowsWrite() {
        return !this.readOnly;

    }

    @Override
    public <T> void setBoxValue(jvstm.VBox<T> vbox, T value) {
        if (!txAllowsWrite()) {
            throw new WriteOnReadError();
        } else {
            numBoxWrites++;
            super.setBoxValue(vbox, value);
        }
    }

    @Override
    public <T> void setPerTxValue(jvstm.PerTxBox<T> box, T value) {
        if (!txAllowsWrite()) {
            throw new WriteOnReadError();
        } else {
            super.setPerTxValue(box, value);
        }
    }

    @Override
    public <T> T getBoxValue(VBox<T> vbox) {
        numBoxReads++;
        return super.getBoxValue(vbox);
    }

    @Override
    protected <T> T getValueFromBody(jvstm.VBox<T> vbox, VBoxBody<T> body) {
        if (body.value == VBox.NOT_LOADED_VALUE) {
            VBox<T> ffVBox = (VBox<T>) vbox;

//            logger.debug("Value for vbox {} is: NOT_LOADED_VALUE", ((VBox) vbox).getId());

            ffVBox.reload();
            // after the reload, the (new) body should have the required loaded value
            // if not, then something gone wrong and its better to abort
            // body = vbox.body.getBody(number);
            body = ffVBox.getBody(getNumber());

            if (body.value == VBox.NOT_LOADED_VALUE) {
                logger.error("Couldn't load the VBox: {}", ffVBox.getId());
                throw new VersionNotAvailableException();
            }
        }

        // notice that body has changed if we went into the previous if 

//        logger.debug("Value for vbox {} is: '{}'", ((VBox) vbox).getId(), body.value);

        return super.getValueFromBody(vbox, body);
    }

    // called when a read from a box detects there is already a newer version.
    @Override
    protected <T> VBoxBody<T> newerVersionDetected(VBoxBody<T> body) {
        // Ill-programmed code such as RadarGun will note handle well an exception mid-transaction :-(
//        if (!this.boxesWritten.isEmpty() || !this.boxesWrittenInPlace.isEmpty()) {
//            return super.newerVersionDetected(body);
//        } else {
        return body.getBody(number);
//        }
    }

    @Override
    public boolean isBoxValueLoaded(VBox vbox) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Transaction makeNestedTransaction(boolean readOnly) {
        throw new Error("Nested transactions not supported yet...");
    }

    /* this method processes the commit requests queue helping to apply as many
    commits as it finds in the queue. This is good for: (1) eventually the queue
    gets processed even if there are only read-only transactions; (2) the
    transactions make an effort to begin in the most up to date state of the
    world, which improves the chances of a write transaction committing successfully.
    This method returns the set of commit requests whose validation could not
    be decided.
    */

    private static CommitRequest processCommitRequests(CommitRequest startingPoint, CommitRequestListener listenerToUse) {
        CommitRequest current = startingPoint;
        CommitRequest previous = current;  // the head is never null

        while (current != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Will handle commit request: {}", current);
            }

            previous = current;
            current = current.handle(listenerToUse);
        }
        return previous;
    }

    @Override
    public void notifyValid(CommitRequest commitRequest) {
        this.undecidedMaybeBenign.remove(commitRequest.getId());
    }

    @Override
    public void notifyUndecided(CommitRequest commitRequest) {
        if (!this.allTestedCommitRequestIds.contains(commitRequest.getId())) {
            this.undecidedMaybeBenign.put(commitRequest.getId(), commitRequest);
        }
//        if (validAgainstWriteSet(commitRequest.getWriteSet())) {
//            this.benignCommitRequestIds.add(commitRequest.getId());
//        }

//        if (!commitRequest.equals(this.myRequestId)) {
//            updateAverageResends(commitRequest.getSendCount() - 1);
//        }
    }

    @Override
    protected void doCommit() {
        if (isWriteTransaction()) {
            TransactionStatistics.STATISTICS.incWrites(this);
        } else {
            TransactionStatistics.STATISTICS.incReads(this);
        }

        if ((numBoxReads > NUM_READS_THRESHOLD) || (numBoxWrites > NUM_WRITES_THRESHOLD)) {
            logger.warn("Very-large transaction (reads = {}, writes = {})", numBoxReads, numBoxWrites);
        }

        // reset statistics counters
        numBoxReads = 0;
        numBoxWrites = 0;

        super.doCommit();
    }

    // statistics
    protected long T1 = 0L;
    protected long T2 = 0L;
    protected long T3 = 0L;
    protected long T4 = 0L;
    protected long T4_1 = 0L;
    protected long T4_2 = 0L;
    protected long T5 = 0L;

    protected int tries = 0;

    protected long beforeValidate;
    protected long afterValidate;
    protected long afterCommitRequestAndPersist;
    protected long afterBroadcast;

    protected long _4_beforeWaitForNext;
    protected long _4_afterWaitForNext;
    protected long _4_afterHandleRequest;

    protected long afterMyCommit;
    protected long afterMappingTxVersion;

    /* This is the main entrance point for the lock-free commit. We override
    tryCommit, and we do not call super.trycommit().  We reuse the commitTx
    machinery in LocalCommitOnlyTransaction, which is the instance that we create
    to decorate LockFreeTransactions from the local node.  In short, here we
    just broadcast a commit request and go process the queue until our
    LocalCommitOnlyTransaction is either committed or found invalid. */
    @Override
    protected void tryCommit() {
        if (isWriteTransaction()) {

            if (this.perTxValues != EMPTY_MAP) {
                logger.error("PerTxValues are not supported in distributed transactions yet.");
                TransactionSignaller.SIGNALLER.signalCommitFail();
                throw new AssertionError("Impossible condition - Commit fail signalled!");
            }

// From ConsistentTopLevelTransaction:
            alreadyChecked = new HashSet();
            checkConsistencyPredicates();
            alreadyChecked = null; // allow gc of set

            CommitRequest myRequest = null;
            ValidationStatus myFinalStatus = null;
            do {

                tries++;

                this.beforeValidate = System.nanoTime();

                this.lastProcessedRequest = preValidateLocally();
                logger.debug("Tx is locally valid");

                this.afterValidate = System.nanoTime();
                this.T1 += (this.afterValidate - this.beforeValidate);

                // compute set of benign requests
                Set<UUID> benignCommitRequestIds = selectBenignRequestsToSend();

//                if (myRequest == null && CommitRequest.contentionExists()) {
//                    long myWait = CommitRequest.getContention();
////                    if (System.currentTimeMillis() % 100 == 0) {
////                    logger.warn("CONTENTION is {}. Waiting for {}ns", CommitRequest.getContention(), myWait);
////                    }
//                    LockFreeTransaction.activeSleep(myWait);
//
//                    lastProcessedRequest = preValidateLocally();
//                    logger.debug("Tx is still locally valid");
//                }

//                logger.warn("At this point contention={}", CommitRequest.getContention());

                if (myRequest == null) { // first try. create the commit request
//                    logger.warn("First commit attempt");
                    myRequest = makeCommitRequest(benignCommitRequestIds);

                    // the myRequest instance will be different, because it is serialized and deserialized. So, just use its ID to identify it.
                    this.myRequestId = myRequest.getId();

                    // persist the write set ahead of sending the commit request
                    persistWriteSet(myRequest);
                } else {
                    myRequest = updateCommitRequest(myRequest, benignCommitRequestIds);
//                    logger.warn("Commit attempts for this tx={}", myRequest.getSendCount());
                }

                this.afterCommitRequestAndPersist = System.nanoTime();
                this.T2 += (this.afterCommitRequestAndPersist - this.afterValidate);

                // clear the contents of the set of undecided requests.  next time we will only include the deltas, so these have been tested anyway
                this.undecidedMaybeBenign = new HashMap<>();

//                if (myRequest.getSendCount() > 5) {
//                    logger.warn("Commit request {} is being sent {} times. Size of benign commits is {}", myRequest.getId(),
//                            myRequest.getSendCount(), myRequest.getBenignCommits().size());
//                }

// From TopLevelTransaction:
//            validate();
//            ensureCommitStatus();
// replaced with:
                CommitRequest myHandledRequest = helpedTryCommit(this.lastProcessedRequest, myRequest);

                this.afterMyCommit = System.nanoTime();
                this.T4 += (this.afterMyCommit - this.afterBroadcast);

                myFinalStatus = myHandledRequest.getValidationStatus();
                logger.debug("My request status was {}", myFinalStatus);

                if (myFinalStatus == ValidationStatus.VALID) {
                    logger.debug("Commit: version={{}}, id={{}}, writeset={{}}", getCommitTxRecord().transactionNumber,
                            this.myRequestId, myHandledRequest.getWriteSet().getVboxIds());

                    // HACK: To avoid timeouts (deadlocks?) in EHCache only the committer writes to the persistence.
                    int txVersion = this.getCommitTxRecord().transactionNumber;
                    String commitId = CommitOnlyTransaction.txVersionToCommitIdMap.get(txVersion);

                    if (commitId != null) { // may be null if it was already persisted 
                        JvstmLockFreeBackEnd.getInstance().getRepository().mapTxVersionToCommitId(txVersion, commitId);
//                        CommitOnlyTransaction.txVersionToCommitIdMap.remove(txVersion);
                    }

                }

                this.lastProcessedRequest = myHandledRequest;

            } while (myFinalStatus != ValidationStatus.VALID); // we're fully written back here

            this.afterMappingTxVersion = System.nanoTime();
            this.T5 += (this.afterMappingTxVersion - this.afterMyCommit);

//            System.out.println("{C} tries=" + this.tries + " T1=" + this.T1 + " T2=" + this.T2 + " T3=" + this.T3 + " T4="
//                    + this.T4 + " T5=" + this.T5);
            System.out.println("{C}," + this.tries + "," + this.T1 + "," + this.T2 + "," + this.T3 + "," + this.T4 + ","
                    + this.T4_1 + "," + this.T4_2 + "," + this.T5);
// From TopLevelTransaction:
            upgradeTx(getCommitTxRecord());  // commitTxRecord was set by the helper LocalCommitOnlyTransaction 
        }
    }

    /* iterates the undecidedMaybeBenign requests and for those that haven't yet
    been tested, checks whether they are benign (i.e. their write set does not
    intersect with my read set).  Side-effect: the set of tested requests is updated */
    private Set<UUID> selectBenignRequestsToSend() {
        Set<UUID> benignIds = new HashSet<>();

        for (CommitRequest commitRequest : this.undecidedMaybeBenign.values()) {
            if (!this.allTestedCommitRequestIds.contains(commitRequest.getId())) {

                this.allTestedCommitRequestIds.add(commitRequest.getId());

                if (validAgainstWriteSet(commitRequest.getWriteSet())) {
                    benignIds.add(commitRequest.getId());
                }
            }

        }
        return benignIds;
    }

    // returns the requests that, even though undecided, could be committed ahead of me without invalidating me
    protected CommitRequest preValidateLocally() {
        // locally validate before continuing
        logger.debug("Validating locally before broadcasting commit request");
//            ActiveTransactionsRecord lastSeenCommitted = helpCommitAll();

        CommitRequest lastProcessedRequest = processCommitRequests(this.lastProcessedRequest, this);

        ActiveTransactionsRecord lastSeenCommitted = Transaction.mostRecentCommittedRecord;
        try {
            snapshotValidation(lastSeenCommitted.transactionNumber);
        } catch (Exception e) {
            StringBuilder str = new StringBuilder();

            str.append("Invalid tx in snapshot validation! ReadSet is:");

            // the first may not be full
            jvstm.VBox[] array = this.bodiesRead.first();
            for (int i = next + 1; i < array.length; i++) {
                String id = ((VBox) array[i]).getId();
                str.append(" {").append(id).append("}");
            }

            // the rest are full
            for (jvstm.VBox[] ar : bodiesRead.rest()) {
                for (jvstm.VBox element : ar) {
                    String id = ((VBox) element).getId();
                    str.append(" {").append(id).append("}");
                }

            }
            logger.debug(str.toString());
        }
        upgradeTx(lastSeenCommitted);

        return lastProcessedRequest;
    }

    // Compute whether this tx is valid against a given write set
    private boolean validAgainstWriteSet(SimpleWriteSet writeSet) {
        for (String vboxId : writeSet.getVboxIds()) {
            if (readSetContains(vboxId)) {
                return false;
            }
        }
        return true;
    }

    // Compute whether this tx's read set contains the given vboxId
    private boolean readSetContains(String vboxId) {
        if (!this.bodiesRead.isEmpty()) {
            // the first may not be full
            jvstm.VBox[] array = this.bodiesRead.first();
            for (int i = next + 1; i < array.length; i++) {
                String id = ((VBox) array[i]).getId();
                if (id.equals(vboxId)) {
                    return true;
                }
            }

            // the rest are full
            for (jvstm.VBox[] ar : bodiesRead.rest()) {
                for (jvstm.VBox element : ar) {
                    String id = ((VBox) element).getId();
                    if (id.equals(vboxId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void persistWriteSet(CommitRequest commitRequest) {
        JvstmLockFreeBackEnd.getInstance().getRepository()
                .persistWriteSet(commitRequest.getId(), commitRequest.getWriteSet(), NULL_VALUE);
    }

    protected CommitRequest helpedTryCommit(CommitRequest lastProcessedRequest, CommitRequest myRequest) throws CommitException {

//        // start by reading the current commit queue's head.  This is to ensure that we don't miss our own commit request
//        CommitRequest currentRequest = LockFreeClusterUtils.getCommitRequestAtHead();

        broadcastCommitRequest(myRequest);
        this.afterBroadcast = System.nanoTime();
        this.T3 += (this.afterBroadcast - this.afterCommitRequestAndPersist);

        return tryCommit(lastProcessedRequest);
    }

    protected static void broadcastSync() {
        logger.warn("Need to send a SYNC request to force the release of the requests buffer!!!");
        LockFreeClusterUtils.sendCommitRequest(CommitRequest.SYNC_REQUEST);
    }

    private UUID broadcastCommitRequest(CommitRequest commitRequest) {
        // for later recovering this transaction
        CommitOnlyTransaction.commitsMap.put(commitRequest.getId(), this);

        LockFreeClusterUtils.sendCommitRequest(commitRequest);
        return commitRequest.getId();
    }

    private CommitRequest makeCommitRequest(Set<UUID> benignCommitRequestIds) {
//        Set<UUID> benignRequestIds = new HashSet<>();
//
//        for (CommitRequest commitRequest : this.benignCommitRequestIds) {
//            benignRequestIds.add(commitRequest.getId());
//        }

//        logger.debug("Will create commit request for transaction. isWriteOnly={}, readSetSize={}", this.bodiesRead.isEmpty()
//                && this.arraysRead.isEmpty(), this.bodiesRead.size() + this.arraysRead.size());
        return new CommitRequest(DomainClassInfo.getServerId(), getNumber(), benignCommitRequestIds, makeSimpleWriteSet(),
                this.bodiesRead.isEmpty() && this.arraysRead.isEmpty());
    }

    private CommitRequest updateCommitRequest(CommitRequest myRequest, Set<UUID> benignCommitRequestIds) {
        return new DeltaCommitRequest(myRequest.getId(), getNumber(), benignCommitRequestIds, myRequest.getSendCount() + 1);

//        myRequest.setBenignCommits(this.benignCommitRequestIds);
//        myRequest.incSendCount();
//        myRequest.setValidTxVersion(getNumber());
    }

//    private SimpleReadSet makeSimpleReadSet() {
//        HashSet<String> vboxIds = new HashSet<String>();
//
//        if (!this.bodiesRead.isEmpty()) {
//            // the first may not be full
//            jvstm.VBox[] array = this.bodiesRead.first();
//            for (int i = next + 1; i < array.length; i++) {
//                String vboxId = ((VBox) array[i]).getId();
//                vboxIds.add(vboxId);
//            }
//
//            // the rest are full
//            for (jvstm.VBox[] ar : bodiesRead.rest()) {
//                for (jvstm.VBox element : ar) {
//                    String vboxId = ((VBox) element).getId();
//                    vboxIds.add(vboxId);
//                }
//            }
//        }
//
//        return new SimpleReadSet(vboxIds.toArray(new String[vboxIds.size()]));
//    }

    private SimpleWriteSet makeSimpleWriteSet() {
        // code adapted from jvstm.WriteSet. It's a bit redundant, and cumbersome to maintain if the original code happens to change :-/  This should be refactored

        // CODE TO DEAL WITH PARALLEL NESTED TRANSACTIONS WAS REMOVED FROM THE ORIGINAL VERSION

        int maxRequiredSize = this.boxesWrittenInPlace.size() + this.boxesWritten.size();

        String[] vboxIds = new String[maxRequiredSize];
        Object[] values = new Object[maxRequiredSize];
        int pos = 0;

        // Deal with VBoxes written in place
        for (jvstm.VBox vbox : this.boxesWrittenInPlace) {
            vboxIds[pos] = ((VBox) vbox).getId();
            values[pos++] = vbox.getInplace().tempValue;
            vbox.getInplace().next = null;
        }

        // Deal with VBoxes written in the fallback write-set
        for (Map.Entry<jvstm.VBox, Object> entry : boxesWritten.entrySet()) {
            jvstm.VBox vbox = entry.getKey();
            if (vbox.getInplace().orec.owner == this) {
                // if we also wrote directly to the box, we just skip this value
                continue;
            }
            vboxIds[pos] = ((VBox) vbox).getId();
            values[pos++] = entry.getValue();
        }

        int writeSetLength = pos;
        return new SimpleWriteSet(Arrays.copyOf(vboxIds, writeSetLength), Arrays.copyOf(values, writeSetLength));
    }

    /**
     * Try to commit everything up to (and including) myRequestId.
     * 
     * @param lastProcessedRequest Head of the remote commits queue
     * @return The {@link CommitRequest} that corresponds to myRequestId
     * @throws CommitException if the validation of my request failed
     */
    protected CommitRequest tryCommit(CommitRequest lastProcessedRequest) throws CommitException {
        // get a transaction and invoke its commit until mine is processed

        CommitRequest current = lastProcessedRequest;
        do {
            this._4_beforeWaitForNext = System.nanoTime();

            current = waitForNextRequest(current);

            this._4_afterWaitForNext = System.nanoTime();
            this.T4_1 += (this._4_afterWaitForNext - this._4_beforeWaitForNext);

            if (logger.isDebugEnabled()) {
                logger.debug("Will handle commit request: {}", current);
            }

            current.handle(this);

            this._4_afterHandleRequest = System.nanoTime();
            this.T4_2 += (this._4_afterHandleRequest - this._4_afterWaitForNext);

            if (current.getId().equals(this.myRequestId)) {
                logger.debug("Processed up to commit request: {}. ValidationStatus: {}", this.myRequestId.toString(),
                        current.getValidationStatus());

                // we're done, regardless of whether we're fully committed
                return current;
            }

        } while (true);
    }

    protected static boolean checkSyncTimeout(long startTime) {
        if (timeElapsed(startTime, SYNC_TIMEOUT)) {
            broadcastSync();
            return true;
        }
        return false;
    }

    protected static boolean timeElapsed(long startTime, long timeout) {
        return System.nanoTime() > startTime + timeout;
    }

    protected static CommitRequest waitForNextRequest(CommitRequest lastProcessedRequest) {
        CommitRequest next = null;
        long startTime = System.nanoTime();
        while ((next = lastProcessedRequest.getNext()) == null) {
//            Thread.yield();
            if (startTime != 0 && checkSyncTimeout(startTime)) {
                startTime = 0;
            }
        }
        return next;
    }

//    /* When this tx is performing local validation before sending its commit
//    record, it will helpcommit those in front of itself that are waiting for
//    write-back.  But, those need to persist their map txversion->commitId also,
//    so we override helpCommit here for that reason. */
//    @Override
//    protected void helpCommit(ActiveTransactionsRecord recordToCommit) {
//        if (!recordToCommit.isCommitted()) {
//            logger.debug("Helping to commit version {}", recordToCommit.transactionNumber);
//
//            int txVersion = recordToCommit.transactionNumber;
//            String commitId = CommitOnlyTransaction.txVersionToCommitIdMap.get(txVersion);
//
//            if (commitId != null) { // may be null if it was already persisted 
//                JvstmLockFreeBackEnd.getInstance().getRepository().mapTxVersionToCommitId(txVersion, commitId);
////                CommitOnlyTransaction.txVersionToCommitIdMap.remove(txVersion);
//            }
//            super.helpCommit(recordToCommit);
//        } else {
//            logger.debug("Version {} was already fully committed", recordToCommit.transactionNumber);
//        }
//    }

}