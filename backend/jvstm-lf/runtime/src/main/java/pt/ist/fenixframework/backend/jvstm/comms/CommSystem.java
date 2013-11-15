package pt.ist.fenixframework.backend.jvstm.comms;

public interface CommSystem {
    public void init(MessageProcessor msgProc);

    public void sendMessage(byte[] data);
}
