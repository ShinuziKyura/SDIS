package dbs;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import dbs.PeerUtility.MessageType;
import rmi.RMIResult;
import util.GenericArrays;
import util.concurrent.LinkedTransientQueue;

public class PeerProtocol implements Runnable {
	private Peer peer;
	private String[] header;
	private byte[] body;

	public PeerProtocol(Peer peer, byte[] message) {
		if (peer.processes.getAndIncrement() < 0) {
			return;
		}
		this.peer = peer;
		String header = new String(message).split("\r\n\r\n", 2)[0];
		this.header = header.split("[ ]+");
		byte[] body = GenericArrays.split(message, header.length())[1];
		this.body = body.length > 4 ? java.util.Arrays.copyOfRange(body, 4, body.length) : null;
	}

	@Override
	public void run() {
		if (peer != null) {
			switch (header[0].toUpperCase()) {
				case "PUTCHUNK":
					backup();
					break;
				case "GETCHUNK":
					restore();
					break;
				case "DELETE":
					delete();
					break;
				case "REMOVED":
					reclaim();
					break;
			}
		}
		peer.processes.decrementAndGet();
	}

	public static RMIResult backup(Peer peer, String filename, String fileID, byte[] file, int replication_degree) {
		if (peer.processes.getAndIncrement() < 0) {
			peer.processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Peer process terminating...");
				return 1;
			});
		}
		if (peer.DBMessages.containsKey(fileID)) {
			peer.processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > PeerUtility.MAXIMUM_FILE_SIZE) {
			peer.processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			peer.processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}

		int stored;
		int requests;
		LinkedTransientQueue<byte[]> replies = new LinkedTransientQueue<>();
		peer.DBMessages.put(fileID, replies);

		String old_fileID = peer.stored_files.put(filename, fileID);
		if (old_fileID != null) {
			peer.executor.execute(new PeerProtocol(peer, PeerUtility.generateProtocolHeader(
					MessageType.DELETE, peer.PROTOCOL_VERSION, peer.ID, old_fileID, null, null)));
		}

		int chunk_count = 0;
		int chunk_amount = file.length / PeerUtility.MAXIMUM_CHUNK_SIZE + 1;
		do {
			int chunk_size = (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE < file.length ?
			                 (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE :
			                 file.length;
			byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                         peer.ID, fileID,
			                                                         chunk_count, replication_degree);
			byte[] chunk_body = Arrays.copyOfRange(file, chunk_count * PeerUtility.MAXIMUM_CHUNK_SIZE, chunk_size);
			byte[] chunk = GenericArrays.join(chunk_header, chunk_body);

			stored = 0;
			requests = 0;
			while (stored < replication_degree && requests < 5) {
				try {
					peer.MDBSocket.send(chunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

			//	replies.init((1 << requests++), TimeUnit.SECONDS);
				replies.init(1 + requests++, TimeUnit.MILLISECONDS); // DEBUG
				while (replies.take() != null) {
					++stored;
				}
			}

			if (stored > 0 && stored < replication_degree) {
				System.out.println("\nWARNING! Replication degree could not be met:" +
				                   "\n\tRequested - " + replication_degree +
				                   "\n\tActual - " + stored);
			}
		} while (++chunk_count < chunk_amount && stored != 0);

		if (stored == 0) {
			old_fileID = peer.stored_files.remove(filename);
			if (chunk_count != 1) {
				peer.executor.execute(new PeerProtocol(peer, PeerUtility.generateProtocolHeader(
						MessageType.DELETE, peer.PROTOCOL_VERSION, peer.ID, old_fileID, null, null)));
			}
		}

		peer.DBMessages.remove(fileID);

		peer.processes.decrementAndGet();

		return (stored != 0 ?
		        new RMIResult<>((args) -> {
			        return 0;
		        }) :
		        new RMIResult<>((args) -> {
			        System.err.println("\nERROR! Chunk " + args[0] + " could not be stored" +
			                           "\nBACKUP protocol terminating...");
			        return 21;
		        }, new String[]{ Integer.toString(chunk_count) }));
	}

	public void backup() {
		// TODO
	}

	public static RMIResult restore(Peer peer, String pathname) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public void restore() {
		// TODO
	}

	public static RMIResult delete(Peer peer, String pathname) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public void delete() {
		// TODO
	}

	public static RMIResult reclaim(Peer peer, int disk_space) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public void reclaim() {
		// TODO
	}
}
