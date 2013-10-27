package pt.ist.fenixframework.backend.jvstm.pstm;

import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;

/**
 * Represents a listener that will be notified when a CommitRequest finishes handling.
 */
public interface CommitRequestListener {

    /**
     * Callback method used by the CommitRequest to notify a committing transaction that such request has been handled and was
     * considered valid.
     */
    public abstract void notifyValid(CommitRequest commitRequest);

    /**
     * Callback method used by the CommitRequest to notify a committing transaction that such request has been handled and left
     * undecided.
     */
    public abstract void notifyUndecided(CommitRequest commitRequest);

}