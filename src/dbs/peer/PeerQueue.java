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
						case "GETCHUNK": // MC
						case "DELETE": // MC
							peer.executor.execute(new PeerProtocol(peer, buffer));
							break;
						case "STORED": // MC
							// Received STORED from another Peer:
							// A backup initiator-protocol may be running: forward messages to that protocol
							if (peer.MDBmessages.containsKey(message_header[3].toUpperCase())) {
								try {
									peer.MDBmessages.get(message_header[3].toUpperCase()).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
							// The chunk may exist on our filesystem: increase count of chunks
							if (peer.chunks_metadata.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4]).incrementAndGet();
								}
								catch (NullPointerException e) {
								}
							}
							break;
						case "CHUNK": // MDR
							// Received CHUNK from another Peer:
							// Forward message to initiator-protocol
							if (peer.MDRmessages.containsKey(message_header[3].toUpperCase())) {
								try {
									peer.MDRmessages.get(message_header[3].toUpperCase()).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
							// Notify any protocol that it no longer needs to send the message
							if (peer.MDRmessages.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.MDRmessages.get(message_header[3].toUpperCase() + "." + message_header[4]).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
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
