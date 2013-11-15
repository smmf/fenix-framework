package pt.ist.fenixframework.backend.jvstm.comms;


public class MessageSender {
//    private final LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
//    private final CommSystem commSystem;
//
//    public MessageSender(CommSystem commSystem) {
//        this.commSystem = commSystem;
//    }
//
//    public void start() {
//        new Thread() {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        //System.out.println("Waiting for something in the queue...");
//                        byte[] data = MessageSender.this.messages.take();
//                        //System.out.println("Found something in the queue...");
//                        MessageSender.this.commSystem.sendMessage(data);
//                        //System.out.println("Sent to commSystem...");
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }.start();
//    }
//
//    public void sendMessage(CommitRequest msg) throws IOException {
//        this.messages.add(msg.toByteArray());
//    }
//
}
