package pt.ist.fenixframework.backend.jvstm.pstm;

import static jvstm.UtilUnsafe.UNSAFE;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import jvstm.ActiveTransactionsRecord;
import jvstm.TopLevelTransaction;
import jvstm.Transaction;
import jvstm.TransactionSignaller;
import jvstm.UtilUnsafe;
import jvstm.WriteSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest.ValidationStatus;
import pt.ist.fenixframework.backend.jvstm.lf.JvstmLockFreeBackEnd;

public abstract class CommitOnlyTransaction extends TopLevelTransaction {

    private static final Logger logger = LoggerFactory.getLogger(CommitOnlyTransaction.class);

    private static final long commitTxRecordOffset = UtilUnsafe.objectFieldOffset(TopLevelTransaction.class, "commitTxRecord");

    /**
     * Maps all node local {@link LockFreeTransaction}s using their id as key. Whoever completes the processing is
     * required to remove the entry from this map, lest it grow indefinitely with the number of transactions.
     * 
     */
    public final static ConcurrentHashMap<UUID, LockFreeTransaction> commitsMap =
            new ConcurrentHashMap<UUID, LockFreeTransaction>();

    public static final ConcurrentHashMap<Integer, ActiveTransactionsRecord> activeRecordsMap = new ConcurrentHashMap<>();

    public static void addToActiveRecordsMap(ActiveTransactionsRecord record) {
        logger.debug("Adding record version {} to activeRecordsMap.", record.transactionNumber);
        ActiveTransactionsRecord previous = CommitOnlyTransaction.activeRecordsMap.putIfAbsent(record.transactionNumber, record);
        // sanity check
        if (previous == null) {
            logger.debug("Success with the mapping.");
        } else if (previous != record) {
            logger.error("There was already another record for this same tx version={}!", record.transactionNumber);
        } else {
            logger.debug("There was a previous association for this record.  That's ok.");
        }
    }

    public static ActiveTransactionsRecord lookupActiveTransactionsRecord(int txVersion) {
        ActiveTransactionsRecord rec = CommitOnlyTransaction.activeRecordsMap.get(txVersion);

        if (rec == null) {
            logger.warn("Warning! Record with version {} not found in the map."
                    + " This can occur only for a remote tx that begun before this node came alive!", txVersion);
        }
        return rec;

    }

    public static final ConcurrentHashMap<Integer, String> txVersionToCommitIdMap =
            new ConcurrentHashMap<Integer, String>(100000);

    protected final CommitRequest commitRequest;

    public CommitOnlyTransaction(ActiveTransactionsRecord record, CommitRequest commitRequest) {
        super(record);
        this.commitRequest = commitRequest;
    }

    @Override
    public boolean isWriteTransaction() {
        return true;
    }

    /**
     * This is the commit algorithm that each CommitOnlyTransaction performs on each node, regardless of whether it is a
     * {@link LocalCommitOnlyTransaction} or a {@link RemoteCommitOnlyTransaction}. Note that {@link LockFreeTransaction}s
     * are decorated by {@link CommitOnlyTransaction}s.
     */
    public void localCommit() {
        // save current
        Transaction savedTx = Transaction.current();
        // set current
        Transaction.current.set(this);
        try {
            // enact the commit
            this.commitTx(false); // smf TODO: double-check whether we want to mess with ActiveTxRecords, thread-locals, etc.  I guess 'false' is the way to go...
        } finally {
            // restore current
            Transaction.current.set(savedTx);
        }
    }

    /**
     * Get the concrete transaction that will be committed. For local commits this should be the local tx instance, help by the
     * LocalCommitOnlyTransaction. For remote commits this should be the RemoteCommitOnlyTransaction instance itself.
     */
    public abstract TopLevelTransaction getUnderlyingTransaction();

    @Override
    protected void assignCommitRecord(int txNumber, WriteSet writeSet) {
        // Must set the correct commit number **BEFORE** setting the valid status
        super.assignCommitRecord(txNumber, writeSet);
        this.commitRequest.setValid();
    }

    @Override
    protected void validate() {
        logger.debug("Validating commit request: {}", this.commitRequest);

        if (this.commitRequest.getIsWriteOnly()) {
            validateWriteOnly();
        } else {
            validateReadWrite();
        }
    }

    protected void validateWriteOnly() {

        // write-only transactions are always valid, so we just need to get the last record where to put them

        ActiveTransactionsRecord existingRec = getLastEnqueuedRecord();

        // first, read whether this request already has a commit record
        ActiveTransactionsRecord thisCommitRecord = this.getCommitTxRecord();

        if (thisCommitRecord == null) { // then it could not have appeared in the enqueued recs.
            // create a record
            assignCommitRecord(existingRec.transactionNumber + 1, getWriteSet());
        }

        /* must always assign a state to ensure visibility*/
        this.commitRequest.setValid();

        /* enqueue this record ahead of the existingRec.  Notice that if existingRec
        is already greater than this request's commit record, then enqueueing
        will (correctly) fail, because it means that some helper already enqueued
        this record */
        enqueueAfter(existingRec);
    }

    protected void validateReadWrite() {
        // these are used to pinpoint the enqueuing location of my record. (just after an existingRec)
        ActiveTransactionsRecord existingRec = lookupActiveTransactionsRecord(this.commitRequest.getValidTxVersion());

        /* In case there was a remote read-write transaction that started in a
        version before this node booted, then the record for that txVersion will
        not be in our map.  In this case, we know that such commit can safely
        be considered undecided.  This is so because, given that such tx hasn't
        been upgraded (during local validation) to include our boot commit, it
        will necessarily appear after it and be considered UNDECIDED by all nodes
        */
        if (existingRec == null) {
            logger.debug("Commit request {} is UNDECIDED", this.commitRequest.getIdWithCount());
            this.commitRequest.setUndecided();
            /* Still, we throw exception to ensure that our own flow does not proceed to enqueueing */
            TransactionSignaller.SIGNALLER.signalCommitFail();
            throw new AssertionError("Impossible condition - Commit fail signalled!");
        }

        ActiveTransactionsRecord nextRec = existingRec.getNext();

        ValidationStatus validationStatus = this.commitRequest.getValidationStatus();
        boolean alreadyValidated = validationStatus != ValidationStatus.UNSET;

        if (alreadyValidated) {
            if (validationStatus == ValidationStatus.VALID) {
                logger.debug("Commit request {} was found already VALID.  Will find insertion point.", this.commitRequest.getId());

                // my commit record must be already assigned because we're already validated
                ActiveTransactionsRecord myCommitRecord = this.getCommitTxRecord();
                // advance until existingRec is just before the expected insertion point of myCommitRecord (which may already be there)
                while (existingRec.transactionNumber < myCommitRecord.transactionNumber - 1) {
                    existingRec = nextRec;
                    nextRec = nextRec.getNext();
                }
            } else {
                logger.debug("Commit request {} was found already UNDECIDED", this.commitRequest.getId());
                /* Still, we throw exception to ensure that our own flow does not proceed to enqueueing */
                TransactionSignaller.SIGNALLER.signalCommitFail();
                throw new AssertionError("Impossible condition - Commit fail signalled!");
            }
        } else {
            while (nextRec != null) {

                UUID nextCommitId = UUID.fromString(txVersionToCommitIdMap.get(nextRec.transactionNumber));

                if (nextCommitId.equals(this.commitRequest.getId())) {  // occurs when meanwhile some helper decided I was valid and inserted me
                    break;
                } else if (!this.commitRequest.getBenignCommits().contains(nextCommitId)) {  // found a non-benign commit record enqueued
                    logger.debug("Unable to validate commit request {}", this.commitRequest.getId());
                    this.commitRequest.setUndecided();
                    TransactionSignaller.SIGNALLER.signalCommitFail();
                    throw new AssertionError("Impossible condition - Commit fail signalled!");
                }

                // move on to the next record
                existingRec = nextRec;
                nextRec = nextRec.getNext();
            }
            assignCommitRecord(existingRec.transactionNumber + 1, getWriteSet());
            logger.debug("Commit request {} is VALID", this.commitRequest.getId());
        }

        enqueueAfter(existingRec);
    }

    private static ActiveTransactionsRecord getLastEnqueuedRecord() {
        ActiveTransactionsRecord existingRec = Transaction.mostRecentCommittedRecord;
        ActiveTransactionsRecord nextRec = existingRec.getNext();

        while (nextRec != null) {
            existingRec = nextRec;
            nextRec = nextRec.getNext();
        }

        return existingRec;
    }

////    @Override
//    protected void validate2() {
////        logger.debug("Validating commit request: {}", this.commitRequest.getId());
//        logger.debug("Validating commit request: {}", this.commitRequest);
//
//        // these are used to pinpoint the enqueuing location of my record. (just after an existingRec)
//        ActiveTransactionsRecord existingRec = lookupActiveTransactionsRecord(this.commitRequest.getValidTxVersion());
//
//        /* In case there was a remote transaction that started in a version
//        before this node booted, then the record for that txVersion will not be
//        in our map.  In this case, we know that such commit can safely be
//        considered valid (if it is write-only) or undecided (otherwise).  This
//        is so because, given that such tx hasn't been upgraded (during local
//        validation) to include our boot commit, it will necessarily appear after
//        it and be considered UNDECIDED by all nodes if it read anything. */
//        if (existingRec == null) {
//            if (this.commitRequest.getIsWriteOnly()) {
//
//                // advance to the first record that exists and use that to search forward. This is safe because the request is write-only and will necessarily be VALID
//                int startVersion = this.commitRequest.getValidTxVersion() + 1;
//                do {
//                    logger.debug("Attempting to find record for version {}", startVersion);
//                    existingRec = lookupActiveTransactionsRecord(startVersion++);
//                } while (existingRec == null);
//
//            } else {
//                logger.debug("Commit request {} is UNDECIDED", this.commitRequest.getId());
//                /* Still, we throw exception to ensure that our own flow does not proceed to enqueueing */
//                TransactionSignaller.SIGNALLER.signalCommitFail();
//                throw new AssertionError("Impossible condition - Commit fail signalled!");
//            }
//        }
//
//        ActiveTransactionsRecord nextRec = existingRec.getNext();
//
//        ValidationStatus validationStatus = this.commitRequest.getValidationStatus();
//        boolean alreadyValidated = validationStatus != ValidationStatus.UNSET;
//
//        if (alreadyValidated) {
//            if (validationStatus == ValidationStatus.VALID) {
//                logger.debug("Commit request {} was found already VALID.  Will find insertion point.", this.commitRequest.getId());
//
//                // my commit record must be already assigned because we're already validated
//                ActiveTransactionsRecord myCommitRecord = this.getCommitTxRecord();
//                // advance until existingRec is just before the expected insertion point of myCommitRecord (which may already be there)
//                while (existingRec.transactionNumber < myCommitRecord.transactionNumber - 1) {
//                    existingRec = nextRec;
//                    nextRec = nextRec.getNext();
//                }
//            } else {
//                logger.debug("Commit request {} was found already UNDECIDED", this.commitRequest.getId());
//                /* Still, we throw exception to ensure that our own flow does not proceed to enqueueing */
//                TransactionSignaller.SIGNALLER.signalCommitFail();
//                throw new AssertionError("Impossible condition - Commit fail signalled!");
//            }
//        } else {
//            while (nextRec != null) {
//
//                UUID nextCommitId = UUID.fromString(txVersionToCommitIdMap.get(nextRec.transactionNumber));
//
//                if (nextCommitId.equals(this.commitRequest.getId())) {  // occurs when meanwhile some helper decided I was valid and inserted me
//                    break;
//                } else if (!this.commitRequest.getIsWriteOnly()                              // only read-write transactions require validation
//                        && !this.commitRequest.getBenignCommits().contains(nextCommitId)) {  // found a non-benign commit record enqueued
//                    logger.debug("Unable to validate commit request {}", this.commitRequest.getId());
//                    this.commitRequest.setUndecided();
//                    TransactionSignaller.SIGNALLER.signalCommitFail();
//                    throw new AssertionError("Impossible condition - Commit fail signalled!");
//                }
//
//                // move on to the next record
//                existingRec = nextRec;
//                nextRec = nextRec.getNext();
//            }
//            logger.debug("Commit request {} is VALID", this.commitRequest.getId());
//        }
//
//        assignCommitRecord(existingRec.transactionNumber + 1, getWriteSet());
//        enqueueAfter(existingRec);
//        updateOrecVersion();
//    }

    @Override
    protected void snapshotValidation(int lastSeenCommittedTxNumber) {
        logger.error("THIS SHOULD NO LONGER BE USED!");
        System.exit(1);
    }

    /**
     * Get the {@link WriteSet} for this transaction.
     * 
     */
    protected abstract WriteSet getWriteSet();

    @Override
    public abstract WriteSet makeWriteSet();

    @Override
    @Deprecated
    protected void validateCommitAndEnqueue(ActiveTransactionsRecord lastCheck) {
        logger.error("THIS SHOULD NO LONGER BE USED!");
        System.exit(1);
    }

    protected void enqueueAfter(ActiveTransactionsRecord lastCheck) {
        enqueueValidCommit(lastCheck, this.getCommitTxRecord().getWriteSet());

        updateOrecVersion();
    }

//    @Override
//    public void updateOrecVersion() {
//        this.getUnderlyingTransaction().updateOrecVersion();
//    }

    @Override
    protected void enqueueValidCommit(ActiveTransactionsRecord lastCheck, WriteSet writeSet) {
        ActiveTransactionsRecord commitRecord = this.getCommitTxRecord();

        CommitOnlyTransaction.addToActiveRecordsMap(commitRecord); // putIfAbsent. ignores if already mapped to some record.

        /* EVERYONE Must try to *putIfAbsent*, to ensure visibility of the
            entry when looking it up ahead.  AND, it must be done before enqueuing
            the ActiveTxRecord, so that whoever reads the volatile 'next' from
            the previous record is guaranteed to the see this mapping. */
        String previous =
                txVersionToCommitIdMap.putIfAbsent(commitRecord.transactionNumber, this.commitRequest.getId().toString());
        logger.debug("Try to associate tx version {} to commitId {} (before was {}).", commitRecord.transactionNumber,
                this.commitRequest.getIdWithCount(), previous);

        /* Here we know that our commit is valid.  However, we may have concluded
        such result via some helper AND even have seen already our record enqueued
        and committed. So we need to check for that to skip enqueuing. */
        if (lastCheck.transactionNumber >= commitRecord.transactionNumber) {
            logger.debug("There is a commit record version {} already in the queue (it better be for commit request {}).",
                    commitRecord.transactionNumber, this.commitRequest.getIdWithCount());
        } else {
            if (lastCheck.trySetNext(commitRecord)) {
                logger.debug("Enqueued record for valid transaction {} of commit request {} (ahead of version {})",
                        commitRecord.transactionNumber, this.commitRequest.getIdWithCount(), lastCheck.transactionNumber);
            } else {
                logger.debug("Commit record version {} of commit request {} was concurrently enqueued by some helper.",
                        commitRecord.transactionNumber, this.commitRequest.getIdWithCount());
            }
        }

    }

    /* The commitTxRecord can only be set once */
    @Override
    public void setCommitTxRecord(ActiveTransactionsRecord record) {
        if (UNSAFE.compareAndSwapObject(this.getUnderlyingTransaction(), this.commitTxRecordOffset, null, record)) {
            logger.debug("set commitTxRecord with version {}", record.transactionNumber);
//            CommitOnlyTransaction.addToActiveRecordsMap(record);
        } else {
            ActiveTransactionsRecord existingRecord = this.getCommitTxRecord();
            logger.debug("commitTxRecord was already set with version {}", existingRecord.transactionNumber);
            // when the CAS fails, another thread may have been preempted just before doing the following line. So we need to ensure that it's done!
//            CommitOnlyTransaction.addToActiveRecordsMap(existingRecord);
        }

    }

//    @Override
//    public ActiveTransactionsRecord getCommitTxRecord() {
//        return this.getUnderlyingTransaction().getCommitTxRecord();
//    }

    @Override
    protected void helpCommit(ActiveTransactionsRecord recordToCommit) {
        if (!recordToCommit.isCommitted()) {

            // HACK: To avoid timeouts (deadlocks?) in EHCache
            if (Thread.currentThread().getName().equals("Commit helper 1")) {
                logger.debug("Helping to commit version {}", recordToCommit.transactionNumber);

                int txVersion = recordToCommit.transactionNumber;
                String commitId = CommitOnlyTransaction.txVersionToCommitIdMap.get(txVersion);

                if (commitId != null) { // may be null if it was already persisted 
                    JvstmLockFreeBackEnd.getInstance().getRepository().mapTxVersionToCommitId(txVersion, commitId);
//                CommitOnlyTransaction.txVersionToCommitIdMap.remove(txVersion);
                }
            }

            super.helpCommit(recordToCommit);
        } else {
            logger.debug("Version {} was already fully committed", recordToCommit.transactionNumber);
        }
    }

    @Override
    protected void upgradeTx(ActiveTransactionsRecord newRecord) {
        // no op.  
        /* This is not a required step in this type of transaction.  The
        corresponding LockFreeTransaction will do this on its own
        node, after all the helping is done. */
    }

}
