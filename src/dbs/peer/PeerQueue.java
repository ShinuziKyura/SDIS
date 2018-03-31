package dbs.peer;

import java.util.concurrent.LinkedTransferQueue;

public class PeerQueue implements Runnable {
    private Peer peer;
	private Thread thread;
	private LinkedTransferQueue<byte[]> queue;

    static final class ChannelQueue {
		private ChannelQueue() {
		}
    }
	private static final ChannelQueue check = new ChannelQueue();

    PeerQueue(Peer peer, PeerChannel channel) {
        this.peer = peer;
        this.queue = channel.queue(check);
    }

    @Override
    public void run() {
    	thread = Thread.currentThread();
        while (peer.instances.get() >= 0) {
            try {
                byte[] buffer = queue.take();

                String[] message_header = new String(buffer).split("\r\n\r\n", 2)[0].split("[ ]+");
                if (!message_header[2].equals(peer.ID)) {
                    switch(message_header[0].toUpperCase()) {
                        case "PUTCHUNK": // MDB
	                        // Received PUTCHUNK from another Peer: Initiate protocol to backup chunk
	                        peer.executor.execute(new PeerProtocol(peer, buffer));
                            break;
                        case "STORED": // MC
	                        // Received STORED from another Peer:
	                        // A backup protocol may be running: forward messages to that protocol
	                        if (peer.DB_messages.containsKey(message_header[3].toUpperCase())) {
	                        	try {
			                        peer.DB_messages.get(message_header[3].toUpperCase()).put(buffer);
		                        }
		                        catch (NullPointerException e) {
		                        }
	                        }
	                        // The chunk may exist on our filesystem: increase count of chunks
	                        if (peer.stored_chunks.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
	                        	try {
			                        peer.stored_chunks.get(message_header[3].toUpperCase() + "." + message_header[4]).incrementAndGet();
		                        }
		                        catch (NullPointerException e) {
		                        }
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
                // Probably terminating
            }
        }
    }

    void stop() {
        thread.interrupt();
    }
}
