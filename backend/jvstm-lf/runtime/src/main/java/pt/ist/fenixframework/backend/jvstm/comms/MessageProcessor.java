package pt.ist.fenixframework.backend.jvstm.comms;

import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;

public interface MessageProcessor {
    public void process(CommitRequest cr);
}
