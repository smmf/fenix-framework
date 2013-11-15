package pt.ist.fenixframework.backend.jvstm.comms.zmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import pt.ist.fenixframework.backend.jvstm.comms.CommSystem;
import pt.ist.fenixframework.backend.jvstm.comms.MessageProcessor;
import pt.ist.fenixframework.backend.jvstm.lf.CommitRequest;

public class ZeroMQCommSystem implements CommSystem {

    private static final Logger logger = LoggerFactory.getLogger(ZeroMQCommSystem.class);

    private ZMQ.Context context;
    private ZMQ.Socket subscriber;
    private ZMQ.Socket request;

    @Override
    public void init(final MessageProcessor msgProc) {
        //  Prepare our context and publisher
        this.context = ZMQ.context(1);

        this.subscriber = this.context.socket(ZMQ.SUB);
        this.subscriber.connect("tcp://localhost:5556");
        this.subscriber.subscribe(new byte[0]);

        this.request = this.context.socket(ZMQ.PUSH);
        this.request.connect("tcp://localhost:5557");

        new Thread() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    byte[] data = ZeroMQCommSystem.this.subscriber.recv(0);

                    CommitRequest msg;
                    try {
                        msg = CommitRequest.makeCommitRequest(data);
                    } catch (IOException e) {
                        logger.error("Failed to create a commit request from the underlying message");
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    msgProc.process(msg);
                }
            }
        }.start();

        // HACK!  Just to see if the connection to the publisher happens before continuing...
        try {
            Thread.sleep(4000);
        } catch (Exception e) {
            // nop
        }
    }

    @Override
    public void sendMessage(byte[] data) {
        System.out.printf("I'm sending a message with %d bytes\n", data.length);
        this.request.send(data, 0);
        //System.out.println("DONE");
    }

    @Override
    public void startSequencer() {
        new Thread(new Sequencer()).start();
    }

    public static class Sequencer implements Runnable {
        @Override
        public void run() {
            //  Prepare our context and publisher
            ZMQ.Context context = ZMQ.context(1);

            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://*:5556");
            //publisher.bind("ipc://weather");

            ZMQ.Socket incoming = context.socket(ZMQ.PULL);
            incoming.bind("tcp://*:5557");

            ZMQ.proxy(incoming, publisher, null);
            // while (! Thread.currentThread().isInterrupted()) {
            //     byte[] data = incoming.recv(0);
            //     //System.out.printf("Received message with %d bytes\n", data.length);
            //     publisher.send(data, 0);
            // }

            publisher.close();
            incoming.close();
            context.term();
        }
    }

}
