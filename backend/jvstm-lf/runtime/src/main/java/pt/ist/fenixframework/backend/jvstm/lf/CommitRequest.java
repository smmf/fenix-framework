/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.lf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import jvstm.CommitException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.pstm.CommitOnlyTransaction;
import pt.ist.fenixframework.backend.jvstm.pstm.CommitRequestListener;
import pt.ist.fenixframework.backend.jvstm.pstm.LocalCommitOnlyTransaction;
import pt.ist.fenixframework.backend.jvstm.pstm.LockFreeTransaction;
import pt.ist.fenixframework.backend.jvstm.pstm.RemoteCommitOnlyTransaction;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class CommitRequest implements DataSerializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(CommitRequest.class);

    public enum ValidationStatus {
        UNSET, VALID, INVALID;  // change INVALID TO UNDECIDED, which is different from UNSET!
    }

    /* for any transaction instance this will always change deterministically
    from UNSET to either VALID or INVALID, i.e. if concurrent helpers try to
    decide, they will conclude the same and the value will never revert back to
    UNSET. */
//    private volatile ValidationStatus validationStatus = ValidationStatus.UNSET; // AtomicReference?
    private final AtomicReference<ValidationStatus> validationStatus = new AtomicReference<ValidationStatus>(
            ValidationStatus.UNSET); // AtomicReference?

    public static synchronized CommitRequest makeSentinelRequest() {
        return new CommitRequest() {
            private static final long serialVersionUID = 2L;

            {
                this.id = new UUID(0, 0);
            }

            @Override
            public ValidationStatus getValidationStatus() {
                return ValidationStatus.VALID;
            };

            @Override
            public void internalHandle() {
                // no-op
            };

            @Override
            public String toString() {
                return "SENTINEL";
            };
        };
    }

    /**
     * A unique request ID.
     */
    protected UUID id;

    /**
     * The serverId from where the request originates.
     */
    private int serverId;

//    /**
//     * The current version of the transaction that creates this commit request.
//     */
//    private int txVersion;

    /**
     * The current version of the transaction that creates this commit request. Also, the version up to which this request has
     * already been validated.
     */
    private int validTxVersion;

    /**
     * A set of commit IDs against which this request is still valid. Even if such commit IDs are validated and committed ahead of
     * this request, they do not affect this request's commit. Basically, it means that the write sets of those commits do not
     * intersect with the read set of the transaction that created this request.
     */
    private Set<UUID> benignCommits;

    private SimpleWriteSet writeSet;

    /**
     * Whether this was a write-only transaction. These transactions will always be valid to commit
     */
    private boolean isWriteOnly;

    /* The following fields are set only by the receiver of the commit request. */

    // The next commit request to process in the queue.
    private final AtomicReference<CommitRequest> next = new AtomicReference<CommitRequest>();
    // The corresponding CommitOnlyTransaction
    private CommitOnlyTransaction transaction;

    public CommitRequest() {
        // required by Hazelcast's DataSerializable
    }

    public CommitRequest(int serverId, /*int txVersion,*/int validTxVersion, Set<UUID> benignCommits, SimpleWriteSet writeSet,
            boolean isWriteOnly) {
        this.id = UUID.randomUUID();
        this.serverId = serverId;
//        this.txVersion = txVersion;
        this.validTxVersion = validTxVersion;
        this.benignCommits = benignCommits;
        this.writeSet = writeSet;
        this.isWriteOnly = isWriteOnly;
    }

    public UUID getId() {
        return this.id;
    }

    public int getServerId() {
        return this.serverId;
    }

//    public int getTxVersion() {
//        return this.txVersion;
//    }

    public int getValidTxVersion() {
        return this.validTxVersion;
    }

    public Set<UUID> getBenignCommits() {
        return this.benignCommits;
    }

    public SimpleWriteSet getWriteSet() {
        return this.writeSet;
    }

    public boolean getIsWriteOnly() {
        return this.isWriteOnly;
    }

    public CommitRequest getNext() {
        return this.next.get();
    }

    public boolean setNext(CommitRequest next) {
        return this.next.compareAndSet(null, next);
    }

    public CommitOnlyTransaction getTransaction() {
        return this.transaction;
    }

    /**
     * Set this request's {@link CommitOnlyTransaction}. It does so based on whether its a local or a remote commit. This
     * method must be called before making the CommitRequest visible to others: This way there is no race in the assignment of
     * this request's transaction.
     */
    public void assignTransaction() {
        LockFreeTransaction tx = CommitOnlyTransaction.commitsMap.remove(this.id);
        if (tx != null) {
            logger.debug("Assigning LocalCommitOnlyTransaction to CommitRequest: {}", this.id);
            this.transaction = new LocalCommitOnlyTransaction(this, tx);
        } else {
            logger.debug("Assigning new RemoteCommitOnlyTransaction to CommitRequest: {}", this.id);
            this.transaction = new RemoteCommitOnlyTransaction(this);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(this.serverId);
//        out.writeInt(this.txVersion);
        writeUUID(out, this.id);

        out.writeInt(this.validTxVersion);

        // if it's a write-only, there is no need to send the set of benign commits. Just send an empty one.
        if (this.isWriteOnly) {
            out.writeInt(0);
        } else {
            out.writeInt(this.benignCommits.size());
            for (UUID commitId : this.benignCommits) {
                writeUUID(out, commitId);
            }
        }

        this.writeSet.writeTo(out);
        out.writeBoolean(this.isWriteOnly);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.serverId = in.readInt();
//        this.txVersion = in.readInt();
        this.id = readUUID(in);

        this.validTxVersion = in.readInt();

        int benignCommitsSize = in.readInt();
        this.benignCommits = new HashSet<UUID>(benignCommitsSize);
        for (int i = 0; i < benignCommitsSize; i++) {
            this.benignCommits.add(readUUID(in));
        }

        this.writeSet = SimpleWriteSet.readFrom(in);
        this.isWriteOnly = in.readBoolean();
    }

    private void writeUUID(ObjectDataOutput out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private UUID readUUID(ObjectDataInput in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("id=").append(this.getId());
//        str.append(", txVersion=").append(this.getTxVersion());
        str.append(", validTxVersion=").append(this.getValidTxVersion());
        str.append(", benignCommits={");
        str.append(this.benignCommits.size());
//        int i = 0;
//        for (UUID uuid : this.benignCommits) {
//            if (i != 0) {
//                str.append(", ");
//            }
//            str.append(uuid.toString());
//            i++;
//        }
        str.append("}");

        str.append(", serverId=").append(this.getServerId());
//        str.append(", readset={");
//        str.append(this.readSet.toString());
//        str.append("}");
        str.append(", writeset={");
        str.append(this.writeSet.toString());
        str.append("}, isWriteOnly={");
        str.append(this.isWriteOnly);
        str.append("}");
        return str.toString();
    }

    /**
     * Handles a commit request. This method is responsible for all the operations required to commit this request. In the end,
     * the request will be marked either as committed or invalid. It may then be removed from the queue of commit requests. It
     * will not be removed from the queue if there is no 'next' request. This is to ensure the invariant: There is always at least
     * one request in the queue. This method never throws an exception. It catches all and always returns the next request to
     * process.
     * 
     * @param helper The {@link LockFreeTransaction} that is helping this request.
     * 
     * @return The next request to process, or <code>null</code> if there are no more records in line.
     */
    @SuppressWarnings("finally")
    public CommitRequest handle(CommitRequestListener helper) {
        CommitRequest next = null;
        try {
            internalHandle();
        } catch (CommitException e) {
            logger.debug("Commit Request {} threw CommitException. Exception will be discarded.", this.getId());
        } catch (Throwable e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Handling localCommit for request {} threw {}.  It will be obfuscated by the return of this method.",
                        this.getId(), e);
                e.printStackTrace();
            }
        } finally {
            if (getValidationStatus() == ValidationStatus.INVALID) {
                helper.notifyUndecided(this);
            }

            next = advanceToNext();
            return next;
        }
    }

    protected void internalHandle() {
        this.getTransaction().localCommit();
    }

    public ValidationStatus getValidationStatus() {
//        return this.validationStatus;
        return this.validationStatus.get();
    }

    /**
     * Mark this commit request as invalid. More than one thread may attempt to set this status. Nevertheless, all threads that
     * attempt it will have the same opinion.
     */
    public void setInvalid() {
//        this.validationStatus = ValidationStatus.INVALID;
        ValidationStatus previous = this.validationStatus.getAndSet(ValidationStatus.INVALID);
        if (previous == ValidationStatus.VALID) {
            String msg = "This is a bug! Validation status must be deterministic!";
            logger.error("msg");
            System.exit(-1);
        }
    }

    /**
     * Mark this commit request as valid. More than one thread may attempt to set this status. Nevertheless, all threads that
     * attempt it will have the same opinion.
     */
    public void setValid() {
//        this.validationStatus = ValidationStatus.VALID;
        ValidationStatus previous = this.validationStatus.getAndSet(ValidationStatus.VALID);
        if (previous == ValidationStatus.INVALID) {
            String msg = "This is a bug! Validation status must be deterministic!";
            logger.error("msg");
            System.exit(-1);
        }
    }

    /**
     * Attempts to remove the current commit request from the head of the queue. It always returns the commit request following
     * this one, which may be <code>null</code> if there is no next request.
     * 
     * @return The commit request following this one (may be <code>null</code> if there is no next request).
     */
    private CommitRequest advanceToNext() {
        /* If we were to return the result of the following method we could skip
        over some commits, in particular, the one we're interested in committing.
        */
        LockFreeClusterUtils.tryToRemoveCommitRequest(this);

        // Always return the commit that really follows
        return this.getNext();
    }

}
