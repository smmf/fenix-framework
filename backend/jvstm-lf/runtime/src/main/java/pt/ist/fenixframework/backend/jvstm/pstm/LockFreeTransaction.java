/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.pstm;

import java.util.Arrays;
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

    private boolean readOnly = false;

    /**
     * Commit requests that may be committed ahead of this transaction, and that do not invalidate it.
     */
    protected Set<CommitRequest> benignCommitRequests;

    // for statistics
    protected int numBoxReads = 0;
    protected int numBoxWrites = 0;

    public LockFreeTransaction(ActiveTransactionsRecord record) {
        super(record);

        this.benignCommitRequests = new HashSet<>();
        CommitOnlyTransaction.addToActiveRecordsMap(record);

        logger.debug("Initial read version is {}", record.transactionNumber);

        upgradeWithPendingCommitsAtBeginning();
    }

    protected void upgradeWithPendingCommitsAtBeginning() {
        processCommitRequests(new CommitRequestListener() {
            @Override
            public void notifyValid(CommitRequest commitRequest) {
                // ignore these notifications when beginning a transaction
            }

            @Override
            public void notifyUndecided(CommitRequest commitRequest) {
                // ignore these notifications when beginning a transaction
            }
        });

        ActiveTransactionsRecord newRecord = Transaction.mostRecentCommittedRecord;
        logger.debug("Done processing pending commit requests.  Most recent version is {}", newRecord.transactionNumber);

        if (newRecord != this.activeTxRecord) {
            logger.debug("Upgrading read version to {}", newRecord.transactionNumber);
            upgradeTx(newRecord);

            CommitOnlyTransaction.addToActiveRecordsMap(newRecord);
        }
    }

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

    private void processCommitRequests(CommitRequestListener listenerToUse) {
        // get a transaction and invoke its commit.
        CommitRequest current = LockFreeClusterUtils.getCommitRequestAtHead();
        while (current != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Will handle commit request: {}", current);
            }

            current = current.handle(listenerToUse);
        }
    }

    @Override
    public void notifyValid(CommitRequest commitRequest) {
        this.benignCommitRequests.remove(commitRequest);
    }

    @Override
    public void notifyUndecided(CommitRequest commitRequest) {
        if (validAgainstWriteSet(commitRequest.getWriteSet())) {
            this.benignCommitRequests.add(commitRequest);
        }
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

            ValidationStatus myFinalStatus = null;
            do {
                preValidateLocally();
                logger.debug("Tx is locally valid");

                // persist the write set ahead of sending the commit request
                CommitRequest myRequest = makeCommitRequest();
                persistWriteSet(myRequest);

// From TopLevelTransaction:
//            validate();
//            ensureCommitStatus();
// replaced with:
                CommitRequest myHandledRequest = helpedTryCommit(myRequest);
                myFinalStatus = myHandledRequest.getValidationStatus();
                logger.debug("My request status was {}", myFinalStatus);
            } while (myFinalStatus != ValidationStatus.VALID); // we're fully written back here

// From TopLevelTransaction:
            upgradeTx(getCommitTxRecord());  // commitTxRecord was set by the helper LocalCommitOnlyTransaction 
        }
    }

    // returns the requests that, even though undecided, could be committed ahead of me without invalidating me
    protected void preValidateLocally() {
        // locally validate before continuing
        logger.debug("Validating locally before broadcasting commit request");
//            ActiveTransactionsRecord lastSeenCommitted = helpCommitAll();

        processCommitRequests(this);

        ActiveTransactionsRecord lastSeenCommitted = Transaction.mostRecentCommittedRecord;
        snapshotValidation(lastSeenCommitted.transactionNumber);
        upgradeTx(lastSeenCommitted);

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

    protected CommitRequest helpedTryCommit(CommitRequest myRequest) throws CommitException {

        // start by reading the current commit queue's head.  This is to ensure that we don't miss our own commit request
        CommitRequest currentRequest = LockFreeClusterUtils.getCommitRequestAtHead();

        UUID myRequestId = broadcastCommitRequest(myRequest);

        // the myRequest instance is different, because it was serialized and deserialized. So, just use its ID.
        return tryCommit(currentRequest, myRequestId);
    }

    private UUID broadcastCommitRequest(CommitRequest commitRequest) {
        // for later recovering this transaction
        CommitOnlyTransaction.commitsMap.put(commitRequest.getId(), this);

        LockFreeClusterUtils.sendCommitRequest(commitRequest);
        return commitRequest.getId();
    }

    private CommitRequest makeCommitRequest() {
        Set<UUID> benignRequestIds = new HashSet<>();

        for (CommitRequest commitRequest : this.benignCommitRequests) {
            benignRequestIds.add(commitRequest.getId());
        }

//        logger.debug("Will create commit request for transaction. isWriteOnly={}, readSetSize={}", this.bodiesRead.isEmpty()
//                && this.arraysRead.isEmpty(), this.bodiesRead.size() + this.arraysRead.size());
        return new CommitRequest(DomainClassInfo.getServerId(), getNumber(), benignRequestIds, makeSimpleWriteSet(),
                this.bodiesRead.isEmpty() && this.arraysRead.isEmpty());
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
     * @param requestToProcess Head of the remote commits queue
     * @param myRequestId My commit request's ID
     * @return The {@link CommitRequest} that corresponds to myRequestId
     * @throws CommitException if the validation of my request failed
     */
    protected CommitRequest tryCommit(CommitRequest requestToProcess, UUID myRequestId) throws CommitException {
        // get a transaction and invoke its commit.

        CommitRequest current = requestToProcess;

        while (true) {
            if (logger.isDebugEnabled()) {
                logger.debug("Will handle commit request: {}", current);
            }

            CommitRequest next = current.handle(this);
            if (current.getId().equals(myRequestId)) {
                logger.debug("Processed up to commit request: {}. ValidationStatus: {}", myRequestId.toString(),
                        current.getValidationStatus());

//                boolean valid = current.getValidationStatus() == ValidationStatus.VALID;
//                if (!valid) {
////                    TransactionSignaller.SIGNALLER.signalCommitFail();
////                    throw new AssertionError("Impossible condition - Commit fail signalled!");
//
//                    /* My validation was undecided. Now I must check whether I'm goo*/
//
//                }

                // we're done, regardless of whether we're fully committed
                return current;
            }

            if (next == null) {
                /* Next is null, so we know that there is nothing more for now.
                We just wait for 'next' to arrive, because we still need to
                handle our own request.  Note that the algorithm is lock-free
                under the assumption that delivery of our own request to our own
                node is guaranteed to occur! */
                logger.debug("Waiting for my own commit request to arrive to the queue.");
                continue;
            } else {
                current = next;
            }
        }
    }

    /* When this tx is performing local validation before sending its commit
    record, it will helpcommit those in front of itself that are waiting for
    write-back.  But, those need to persist their map txversion->commitId also,
    so we override helpCommit here for that reason. */
    @Override
    protected void helpCommit(ActiveTransactionsRecord recordToCommit) {
        if (!recordToCommit.isCommitted()) {
            logger.debug("Helping to commit version {}", recordToCommit.transactionNumber);

            int txVersion = recordToCommit.transactionNumber;
            String commitId = CommitOnlyTransaction.txVersionToCommitIdMap.get(txVersion);

            if (commitId != null) { // may be null if it was already persisted 
                JvstmLockFreeBackEnd.getInstance().getRepository().mapTxVersionToCommitId(txVersion, commitId);
                CommitOnlyTransaction.txVersionToCommitIdMap.remove(txVersion);
            }
            super.helpCommit(recordToCommit);
        } else {
            logger.debug("Version {} was already fully committed", recordToCommit.transactionNumber);
        }
    }

}
