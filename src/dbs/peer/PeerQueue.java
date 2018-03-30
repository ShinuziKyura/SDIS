package dbs.peer;

import java.util.concurrent.LinkedTransferQueue;

public class PeerQueue implements Runnable {
    private Peer peer;
    private LinkedTransferQueue<byte[]> queue;

    public static final class ChannelQueue {
		private ChannelQueue() {
		}
    }
	private static final ChannelQueue check = new ChannelQueue();

    public PeerQueue(Peer peer, PeerChannel channel) {
        this.peer = peer;
        this.queue = channel.queue(check);
    }

    @Override
    public void run() {
        while (peer.running.get()) {
            try {
                byte[] buffer = queue.take();
                String[] message_header = new String(buffer).split("\r\n\r\n", 2)[0].split("[ ]+");
                if (!message_header[2].equals(peer.ID)) {
                    switch(message_header[0].toUpperCase()) {
                        case "PUTCHUNK": // MDB
                            if (!peer.DBMessages.containsKey(message_header[3])) {
                                peer.executor.execute(new PeerProtocol(peer, buffer));
                            }
                            break;
                        case "STORED": // MC
                            if (peer.DBMessages.containsKey(message_header[3])) {
                                peer.DBMessages.get(message_header[3]).put(buffer);
                            }
                            if (peer.stored_chunks.containsKey(message_header[3])) {
                                peer.stored_chunks.get(message_header[3]).incrementAndGet();
                            }
                            break;
                        case "GETCHUNK": // MC

                            break;
                        case "CHUNK": // MDR

                            break;
                        case "DELETE": // MC

                            break;
                        case "REMOVED": // MC

                            break;
                    }
                }
            }
            catch (InterruptedException e) {
                // Shouldn't happen
            }
        }
    }

    public void stop() {
        queue.put(PeerUtility.generateProtocolHeader(PeerUtility.MessageType.STOP, peer.PROTOCOL_VERSION, peer.ID,
                                                     "null", null, null));
    }
}