package dbs;

import util.concurrent.LinkedTransientQueue;
import dbs.PeerUtility.ChunkMetadata;

import static nio.channels.DatagramChannel.DatagramPackage;

public class PeerDispatcher implements Runnable {
	private Peer peer;
	private PeerReceiver receiver;

	PeerDispatcher(Peer peer, PeerReceiver receiver) {
		this.peer = peer;
		this.receiver = receiver;
	}

	@Override
	public void run() {
		while (peer.running.get()) {
			DatagramPackage buffer = receiver.receive();

			if (buffer == null) {
				break;
			}

			String[] message_header = new String(buffer.message).split("\r\n\r\n", 2)[0].split("[ ]+");
			if (!message_header[2].equals(peer.ID)) {
				switch(message_header[0].toUpperCase()) {
					case "PUTCHUNK": { // MDB
						LinkedTransientQueue<byte[]> messages;
						// A reclaim protocol may be running: notify that protocol that it no longer needs to store the file
						if ((messages = peer.reclaim_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
							messages.put(buffer.message);

							break; // <----- You guys probably won't understand why, but this "break" is the single most brilliant idea ever...
						}
					}
					case "DELETE": // MC
						peer.executor.execute(new PeerProtocol(peer, buffer));
						break;
					case "GETCHUNK": // MC
						switch (peer.PROTOCOL_VERSION.toString()) {
							case "1.0":
								peer.executor.execute(new PeerProtocol(peer, buffer));
								break;
							case "1.1":
								peer.executor.execute(new PeerProtocol(peer, buffer));
								break;
						}
						break;
					case "STORED": { // MC
						LinkedTransientQueue<byte[]> messages;
						ChunkMetadata chunkmetadata;
						// Received STORED from another Peer:
						// A backup initiator-protocol may be running: forward messages to that protocol
						if ((messages = peer.backup_messages.get(message_header[3].toUpperCase())) != null) {
							messages.put(buffer.message);
						}
						switch (peer.PROTOCOL_VERSION.toString()) {
							case "1.1":
								// An enhanced backup protocol may be running: forward messages to that protocol
								if ((messages = peer.backup_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
									messages.put(buffer.message);
								}
								break;
						}
						// A reclaim protocol may be running: forward messages to that protocol
						if ((messages = peer.reclaim_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
							messages.put(buffer.message);
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
							messages.put(buffer.message);
						}
						// A restore protocol may be running: notify that protocol that it no longer needs to send the file
						else if ((messages = peer.restore_messages.get(message_header[3].toUpperCase() + "." + message_header[4])) != null) {
							messages.put(buffer.message);
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
	}
}
