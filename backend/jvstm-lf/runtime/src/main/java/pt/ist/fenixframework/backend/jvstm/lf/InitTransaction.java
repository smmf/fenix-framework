/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.lf;

import jvstm.ActiveTransactionsRecord;
import jvstm.CommitException;
import jvstm.TransactionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.pstm.CommitOnlyTransaction;
import pt.ist.fenixframework.backend.jvstm.pstm.LockFreeTransaction;

public class InitTransaction extends LockFreeTransaction {

    private static final Logger logger = LoggerFactory.getLogger(InitTransaction.class);

    /* used to store the most recent persisted version before the commit request
    is sent.  This way we later know that it's enough to lookup commit higher
    than this one to find out our own commit version.*/
    private int existingVersion;

    public InitTransaction(ActiveTransactionsRecord record) {
        super(record);
    }

    @Override
    protected void upgradeWithPendingCommitsAtBeginning() {
        // no-op
        /* we cannot help while initializing... :-) */
    }

    @Override
    protected CommitRequest preValidateLocally() {
        // no-op
        /* this requires helping and we can't do it while initializing. Just return the commit request at the front. */

        return LockFreeClusterUtils.getCommitRequestAtHead();
    }

    @Override
    protected CommitRequest helpedTryCommit(CommitRequest lastProcessedRequest, CommitRequest myRequest) throws CommitException {
        this.existingVersion = JvstmLockFreeBackEnd.getInstance().getRepository().getMaxCommittedTxNumber();

        return super.helpedTryCommit(lastProcessedRequest, myRequest);
    }

    @Override
    protected CommitRequest tryCommit(CommitRequest lastProcessedRequest) throws CommitException {
        // discard all commit request up to mine
        while (!lastProcessedRequest.getId().equals(this.myRequestId)) {
            logger.debug("Ignoring commit request: {}", lastProcessedRequest.getId());

            lastProcessedRequest = LockFreeClusterUtils.tryToRemoveCommitRequest(lastProcessedRequest);
        }

        /* wait until I see my request committed. It will necessarily have a
        number higher than the one committed before I sent my request, so no
        need to look to numbers lower than that */

        int versionToLookup = this.existingVersion + 1;
        String myRequestIdString = this.myRequestId.toString();
        String commitId;
        do {
            commitId = JvstmLockFreeBackEnd.getInstance().getRepository().getCommitIdFromVersion(versionToLookup);
            if (commitId != null) {
                // next try, if needed, will be for a higher version
                versionToLookup++;
            } else {
                logger.info("Waiting for version {} to have a commitId", versionToLookup);
                // wait a little before retrying
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (!myRequestIdString.equals(commitId));

        /* versionToLookup-1 is the version in which we found our initialization
        commit persisted.  When we see that number set in the repository, it's
        already committed, so just set versionToLoad-1 as the first one*/

        // fill in this commit requests validation status, because others may see this as the sentinel
        assignCommitRecord(versionToLookup - 1, makeWriteSet());
        CommitOnlyTransaction.addToActiveRecordsMap(this.commitTxRecord); // make this record available as starting point for future commits
        lastProcessedRequest.setValid();

        // set the most recent record
        TransactionUtils.initializeTxNumber(versionToLookup - 1);

        // we don't need it in the queue anymore (this is optional)
        LockFreeClusterUtils.tryToRemoveCommitRequest(lastProcessedRequest);

        return lastProcessedRequest;
    }

}
