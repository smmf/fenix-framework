package pt.ist.fenixframework.backend.jvstm.comms.hazelcast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.comms.CommSystem;
import pt.ist.fenixframework.backend.jvstm.comms.MessageProcessor;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;
import pt.ist.fenixframework.backend.jvstm.lf.LockFreeClusterUtils;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

public class HazelcastCommSystem implements CommSystem {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastCommSystem.class);

    @Override
    public void init(final MessageProcessor msgProc) {
        ITopic<byte[]> topic = LockFreeClusterUtils.getHazelcastInstance().getTopic(LockFreeClusterUtils.FF_COMMIT_TOPIC_NAME);

        topic.addMessageListener(new MessageListener<byte[]>() {
            @Override
            public void onMessage(Message<byte[]> message) {
                CommitRequest m;
                try {
                    m = CommitRequest.makeCommitRequest(message.getMessageObject());
                } catch (IOException e) {
                    logger.error("Failed to create a commit request from the underlying message");
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                msgProc.process(m);
            }
        });
    }

    @Override
    public void sendMessage(byte[] data) {
        ITopic<byte[]> topic = LockFreeClusterUtils.getHazelcastInstance().getTopic(LockFreeClusterUtils.FF_COMMIT_TOPIC_NAME);
        topic.publish(data);
    }
}
