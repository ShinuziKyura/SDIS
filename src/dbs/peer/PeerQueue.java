package dbs.peer;

import java.util.concurrent.LinkedTransferQueue;

import dbs.util.concurrent.LinkedTransientQueue;

import dbs.peer.PeerUtility.ChunkMetadata;

public class PeerQueue implements Runnable {
	private Peer peer;
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
		while (peer.running.get()) {
			try {
				byte[] buffer = queue.take();

				if (new String(buffer).equals("STOP")) {
					break;
				}

				String[] message_header = new String(buffer).split("\r\n\r\n", 2)[0].split("[ ]+");
				if (!message_header[2].equals(peer.ID)) {
					switch(message_header[0].toUpperCase()) {
						case "PUTCHUNK": { // MDB
							LinkedTransientQueue<byte[]> messages;
							// A reclaim protocol may be running: notify that protocol that it no longer needs to store the file
							if ((messages = peer.reclaim_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								messages.put(buffer);

								break; // <----- You guys probably won't understand why, but THIS "break;" is the single most brilliant idea ever...
							}
						}
						case "GETCHUNK": // MC
						case "DELETE": // MC
							peer.executor.execute(new PeerProtocol(peer, buffer));
							break;
						case "STORED": { // MC
							LinkedTransientQueue<byte[]> messages;
							ChunkMetadata chunkmetadata;
							// Received STORED from another Peer:
							// A backup initiator-protocol may be running: forward messages to that protocol
							if ((messages = peer.backup_messages.get(message_header[3].toUpperCase())) != null) {
								messages.put(buffer);
							}
							// A reclaim protocol may be running: forward messages to that protocol
							if ((messages = peer.reclaim_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								messages.put(buffer);
							}
							// The chunk may exist on our filesystem: increase count of chunks
							if ((chunkmetadata = peer.local_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								chunkmetadata.perceived_replication.add(message_header[2]);
							}
							// The chunk may belong to a file we backed up: increase count of chunks
							else if ((chunkmetadata = peer.remote_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								chunkmetadata.perceived_replication.add(message_header[2]);
							}
							break;
						}
						case "CHUNK": { // MDR
							LinkedTransientQueue<byte[]> messages;
							// Received CHUNK from another Peer:
							// A restore initiator-protocol may be running: forward message to that protocol
							if ((messages = peer.restore_messages.get(message_header[3].toUpperCase())) != null) {
								messages.put(buffer);
							}
							// A restore protocol may be running: notify that protocol that it no longer needs to send the file
							else if ((messages = peer.restore_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								messages.put(buffer);
							}
							break;
						}
						case "REMOVED": { // MC
							ChunkMetadata chunkmetadata;
							// The chunk may exist on our filesystem: decrease count of chunks and start reclaim protocol
							if ((chunkmetadata = peer.local_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								chunkmetadata.perceived_replication.remove(message_header[2]);

								peer.executor.execute(new PeerProtocol(peer, buffer));
							}
							// The chunk may belong to a file we backed up: decrease count of chunks
							else if ((chunkmetadata = peer.remote_chunks_metadata.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
								chunkmetadata.perceived_replication.remove(message_header[2]);
							}
							break;
						}
					}
				}
			}
			catch (InterruptedException e) {
			}
		}
	}

	void stop() {
		queue.clear();
		queue.put("STOP".getBytes());
	}
}
