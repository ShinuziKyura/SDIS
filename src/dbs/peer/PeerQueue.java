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
							// A reclaim protocol may be running: forward messages to that protocol
							if (peer.backup_messages.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.backup_messages.get(message_header[3].toUpperCase() + "." + message_header[4]).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
						case "GETCHUNK": // MC
						case "DELETE": // MC
							peer.executor.execute(new PeerProtocol(peer, buffer));
							break;
						case "STORED": // MC
							// Received STORED from another Peer:
							// A backup initiator-protocol or reclaim protocol may be running: forward messages to that protocol
							if (peer.backup_messages.containsKey(message_header[3].toUpperCase())) {
								try {
									peer.backup_messages.get(message_header[3].toUpperCase()).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
							// The chunk may exist on our filesystem: increase count of chunks
							if (peer.local_chunks_metadata.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.local_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4]).perceived_replication.add(message_header[2]);
								}
								catch (NullPointerException e) {
								}
							}
							// The chunk may belong to a file we backed up: increase count of chunks
							else if (peer.remote_chunks_metadata.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.remote_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4]).perceived_replication.add(message_header[2]);
								}
								catch (NullPointerException e) {
								}
							}
							break;
						case "CHUNK": // MDR
							// Received CHUNK from another Peer:
							// A restore initiator-protocol may be running: forward message to that protocol
							if (peer.restore_messages.containsKey(message_header[3].toUpperCase())) {
								try {
									peer.restore_messages.get(message_header[3].toUpperCase()).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
							// A restore protocol may be running: notify that protocol that it no longer needs to send the message
							if (peer.restore_messages.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.restore_messages.get(message_header[3].toUpperCase() + "." + message_header[4]).put(buffer);
								}
								catch (NullPointerException e) {
								}
							}
							break;
						case "REMOVED": // MC
							// The chunk may exist on our filesystem: decrease count of chunks and start reclaim protocol
							if (peer.local_chunks_metadata.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.local_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4]).perceived_replication.remove(message_header[2]);
								}
								catch (NullPointerException e) {
								}
								peer.executor.execute(new PeerProtocol(peer, buffer));
							}
							// The chunk may belong to a file we backed up: decrease count of chunks
							else if (peer.remote_chunks_metadata.containsKey(message_header[3].toUpperCase() + "." + message_header[4])) {
								try {
									peer.remote_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4]).perceived_replication.remove(message_header[2]);
								}
								catch (NullPointerException e) {
								}
							}
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
