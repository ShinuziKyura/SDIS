package dbs.peer;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dbs.peer.PeerUtility.MessageType;
import dbs.rmi.RemoteFunction;
import dbs.util.GenericArrays;
import dbs.util.concurrent.LinkedTransientQueue;

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
		if (peer == null) {
			peer.processes.decrementAndGet();
			return;
		}

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

		peer.processes.decrementAndGet();
	}

	public static RemoteFunction backup(Peer peer, String filename, String fileID, byte[] file, int replication_degree) {
		if (peer.processes.getAndIncrement() < 0) {
			peer.processes.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Peer process terminating...");
				return 1;
			});
		}
		if (peer.DB_messages.containsKey(fileID)) {
			peer.processes.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > PeerUtility.MAXIMUM_FILE_SIZE) {
			peer.processes.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			peer.processes.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}
		if (peer.stored_files.containsKey(filename) || peer.stored_chunks.containsKey(fileID)) {
			peer.processes.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! File already exists in the service" +
				                   "\nBACKUP protocol terminating...");
				return 13;
			});
		}

		int stored;
		int requests;
		LinkedTransientQueue<byte[]> replies = new LinkedTransientQueue<>();
		peer.DB_messages.put(fileID, replies);

		peer.stored_files.put(filename, fileID);

		int chunk_count = 0;
		int chunk_amount = file.length / PeerUtility.MAXIMUM_CHUNK_SIZE + 1;
		do {
			HashSet<String> stored_peers = new HashSet<>();

			String chunk_name = fileID + "." + chunk_count;
			int chunk_size = (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE < file.length ?
			                 (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE :
			                 file.length;
			byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                         peer.ID, fileID,
			                                                         chunk_count, replication_degree);
			byte[] chunk_body = Arrays.copyOfRange(file, chunk_count * PeerUtility.MAXIMUM_CHUNK_SIZE, chunk_size);
			byte[] chunk = GenericArrays.join(chunk_header, chunk_body);

			peer.stored_chunks.put(chunk_name, new AtomicInteger(0));

			stored = 0;
			requests = 0;
			while (stored < replication_degree && requests < 5) {
				byte[] message;
				try {
					peer.MDBSocket.send(chunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				//	replies.init((1 << requests++), TimeUnit.SECONDS);
				replies.init(1 + requests++, TimeUnit.MILLISECONDS); // DEBUG
				while ((message = replies.take()) != null) {
					String[] header = new String(message).split("[ ]+");
					if (!stored_peers.contains(header[2]) && chunk_name.equals(header[3].toUpperCase() + "." + header[4])) {
						stored_peers.add(header[2]);
						++stored;
					}
				}
			}

			if (stored > 0 && stored < replication_degree) {
				System.out.println("\nWARNING! Replication degree could not be met:" +
				                   "\n\tRequested - " + replication_degree +
				                   "\n\tActual - " + stored);
			}
		} while (++chunk_count < chunk_amount && stored != 0);

		if (stored == 0) {
			String failed_fileID = peer.stored_files.remove(filename);
			while (--chunk_count >= 0) {
				peer.stored_chunks.remove(failed_fileID + "." + chunk_count);
			}

			if (chunk_count != 1) {
				peer.executor.execute(new PeerProtocol(peer, PeerUtility.generateProtocolHeader(
						MessageType.DELETE, peer.PROTOCOL_VERSION,
						peer.ID, failed_fileID,
						null, null)));
			}
		}

		peer.DB_messages.remove(fileID);

		peer.processes.decrementAndGet();

		return (stored != 0 ?
		        new RemoteFunction<>((args) -> {
			        return 0;
		        }) :
		        new RemoteFunction<>((args) -> {
			        System.err.println("\nERROR! Chunk " + args[0] + " could not be stored" +
			                           "\nBACKUP protocol terminating...");
			        return 21;
		        }, new String[]{ Integer.toString(chunk_count) }));
	}

	public void backup() {
		String filename = header[3].toUpperCase() + "." + header[4];
		if (!peer.stored_chunks.containsKey(filename)) {
			File file = new File("src/dbs/peer/data/" + filename);

			try {
				if (!file.createNewFile()) {
					// File already exists; can't risk corrupting existing files
					System.err.println("\nERROR! File already exists" +
					                   "\nBACKUP protocol terminating...");
					return;
				}
			}
			catch (IOException e) {
				// Something else went wrong; again, can't risk corrupting existing files
				System.err.println("\nERROR! File creation failed" +
				                   "\nBACKUP protocol terminating...");
				return;
			}

			try (FileOutputStream file_stream = new FileOutputStream(file)) {
				file_stream.write(body);
			}
			catch (IOException e) {
			}

			peer.stored_chunks.put(filename, new AtomicInteger(1));
		}

		try {
			int duration = ThreadLocalRandom.current().nextInt(401);
			TimeUnit.MILLISECONDS.sleep(duration);

			byte[] message = PeerUtility.generateProtocolHeader(MessageType.STORED, peer.PROTOCOL_VERSION,
			                                                    peer.ID, header[3].toUpperCase(),
			                                                    Integer.valueOf(header[4]), null);
			peer.MCSocket.send(message);
		}
		catch (IOException | InterruptedException e) {
			// Shouldn't happen
		}
	}

	public static RemoteFunction restore(Peer peer, String pathname) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	public void restore() {
		// TODO
	}

	public static RemoteFunction delete(Peer peer, String pathname) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	public void delete() {
		// TODO
	}

	public static RemoteFunction reclaim(Peer peer, int disk_space) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	public void reclaim() {
		// TODO
	}
}
