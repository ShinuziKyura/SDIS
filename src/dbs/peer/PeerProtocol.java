package dbs.peer;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dbs.peer.PeerUtility.MessageType;
import dbs.peer.PeerUtility.FileInfo;
import dbs.rmi.RemoteFunction;
import dbs.util.GenericArrays;
import dbs.util.concurrent.LinkedTransientQueue;

public class PeerProtocol implements Runnable {
	private Peer peer;
	private String[] header;
	private byte[] body;

	public PeerProtocol(Peer peer, byte[] message) {
		if (peer.instances.getAndIncrement() < 0) {
			return;
		}
		this.peer = peer;

		String header = new String(message).split("\r\n\r\n", 2)[0];
		this.header = header.split("[ ]+");

		byte[] body = GenericArrays.split(message, header.getBytes().length)[1];
		this.body = body.length > 4 ? Arrays.copyOfRange(body, 4, body.length) : new byte[]{};
	}

	@Override
	public void run() {
		if (peer == null) {
			peer.instances.decrementAndGet();
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

		peer.instances.decrementAndGet();
	}

	static RemoteFunction backup(Peer peer, String filename, String fileID, byte[] file, int replication_degree) {
		if (peer.instances.getAndIncrement() < 0) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Peer process terminating...");
				return 1;
			});
		}
		if (peer.MDBmessages.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > PeerUtility.MAXIMUM_FILE_SIZE) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}
		if (peer.files_metadata.containsKey(filename) || peer.chunks_metadata.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File already exists in the service metadata" +
				                   "\nBACKUP protocol terminating...");
				return 13;
			});
		}

		LinkedList<String> warnings = new LinkedList<>();

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
		peer.MDBmessages.put(fileID, messages);

		int replies;
		int chunk_number = 0;
		int chunk_amount = file.length / PeerUtility.MAXIMUM_CHUNK_SIZE + 1;
		peer.files_metadata.put(filename, new FileInfo(fileID, chunk_amount, replication_degree));
		do {
			HashSet<String> stored_peers = new HashSet<>();

			String chunkID = fileID + "." + chunk_number;
			int putchunk_body_length = (chunk_number + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE < file.length ?
			                           (chunk_number + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE :
			                           file.length;
			byte[] putchunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                            peer.ID, fileID,
			                                                            chunk_number, replication_degree);
			byte[] putchunk_body = Arrays.copyOfRange(file, chunk_number * PeerUtility.MAXIMUM_CHUNK_SIZE, putchunk_body_length);
			byte[] putchunk = GenericArrays.join(putchunk_header, putchunk_body);

			peer.chunks_metadata.put(chunkID, new AtomicInteger(0));

			replies = 0;
			int requests = 0;
			while (replies < replication_degree && requests < 5) {
				try {
					peer.MDBsocket.send(putchunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				byte[] stored;
				messages.init((1 << requests++), TimeUnit.SECONDS);
				while ((stored = messages.poll()) != null) {
					String[] stored_header = new String(stored).split("[ ]+");

					if (!stored_peers.contains(stored_header[2]) && chunkID.equals(stored_header[3].toUpperCase() + "." + stored_header[4])) {
						stored_peers.add(stored_header[2]);
						++replies;
					}
				}
			}

			if (replies < replication_degree) {
				warnings.add("\nWARNING! Replication degree of chunk " + chunk_number + " could not be met:" +
				             "\n\tRequested - " + replication_degree +
				             "\n\tActual - " + replies);
			}
		} while (replies > 0 && ++chunk_number < chunk_amount);

		int failed_chunk_number = 0; // Placebo
		if (replies == 0) {
			failed_chunk_number = chunk_number;

			peer.files_metadata.remove(filename);
			peer.chunks_metadata.remove(fileID);

			do {
				peer.chunks_metadata.remove(fileID + "." + chunk_number);
			} while (--chunk_number >= 0);

			if (failed_chunk_number > 0) {
				peer.executor.execute(new PeerProtocol(peer, PeerUtility.generateProtocolHeader(MessageType.DELETE, peer.PROTOCOL_VERSION,
				                                                                                peer.ID, fileID,
				                                                                                null, null)));
			}
		}

		peer.MDBmessages.remove(fileID);

		peer.instances.decrementAndGet();

		return (replies > 0 ?
		        new RemoteFunction<>((args) -> {
			        for (String arg : (LinkedList<String>) args[0]) {
				        System.out.println(arg);
			        }
		        	System.out.println("\nSUCCESS! File stored");
			        return 0;
		        }, new Object[]{ warnings }) :
		        new RemoteFunction<>((args) -> {
			        System.err.println("\nFAILURE! Chunk " + args[0] + " could not be stored" +
			                           "\nBACKUP protocol terminating...");
			        return 14;
		        }, new Object[]{ failed_chunk_number }));
	}

	private void backup() {
		String filename = header[3].toUpperCase() + "." + header[4];
		Path pathname = Paths.get("src/dbs/peer/data/" + filename);
		if (peer.chunks_metadata.putIfAbsent(filename, new AtomicInteger(1)) == null) {
			try {
				Files.write(pathname, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.DSYNC);
			}
			catch (IOException e) {
				peer.chunks_metadata.remove(filename);
				// File already exists; can't risk corrupting existing files
				System.err.println("\nFAILURE! File already exists" +
				                   "\nBACKUP protocol terminating...");
				return;
			}
		}

		try {
			int duration = ThreadLocalRandom.current().nextInt(401);
			TimeUnit.MILLISECONDS.sleep(duration);

			byte[] message = PeerUtility.generateProtocolHeader(MessageType.STORED, peer.PROTOCOL_VERSION,
			                                                    peer.ID, header[3],
			                                                    Integer.valueOf(header[4]), null);
			peer.MCsocket.send(message);
		}
		catch (IOException | InterruptedException e) {
			// Shouldn't happen
		}
	}

	/*
	 * @return Object[] with status code at index 0 (Integer) and file at index 1 (byte[])
	 */
	static RemoteFunction restore(Peer peer, String filename) {
		if (peer.instances.getAndIncrement() < 0) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Peer process terminating...");
				return new Object[]{ 1, new byte[]{} };
			});
		}

		String fileID;
		if ((fileID = peer.files_metadata.get(filename).fileID) == null) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File does not exist in this service metadata" +
				                   "\nRESTORE protocol terminating...");
				return new Object[]{ 21, new byte[]{} };
			});
		}

		byte[] file = new byte[]{};

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
		peer.MDRmessages.put(fileID, messages);

		// Works the same way as backup but it is supposed to be TCP
		byte[] chunk;
		int chunk_number = 0;
		int chunk_amount = peer.files_metadata.get(filename).chunk_amount;
		do {
			byte[] getchunk = PeerUtility.generateProtocolHeader(MessageType.GETCHUNK, peer.PROTOCOL_VERSION,
			                                                     peer.ID, fileID,
			                                                     chunk_number, null);

			chunk = null;
			int requests = 0;
			while (chunk == null && requests < 5) {
				try {
					peer.MCsocket.send(getchunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				messages.init((1 << requests++), TimeUnit.SECONDS);
				while ((chunk = messages.poll()) != null) {
					String[] chunk_header = new String(chunk).split("\r\n\r\n", 2);
					int chunk_header_length = chunk_header[0].length() + 4;
					chunk_header = chunk_header[0].split("[ ]+");

					if (fileID.equals(chunk_header[3].toUpperCase()) && chunk_number == Integer.valueOf(chunk_header[4])) {
						file = GenericArrays.join(file, Arrays.copyOfRange(chunk, chunk_header_length, chunk.length));
						// We got the chunk we want, break from this loop
						break;
					}
				}
			}
		} while(chunk != null && ++chunk_number < chunk_amount);

		peer.MDRmessages.remove(fileID);

		return (chunk != null ?
		        new RemoteFunction<>((args) -> {
		        	System.out.println("\nSUCCESS! File restored");
		        	return new Object[] { 0, args[0] };
		        }, new Object[] { file }) :
		        new RemoteFunction<>((args) -> {
			        System.err.println("\nFAILURE! Chunk " + args[0] + " could not be restored" +
			                           "\nRESTORE protocol terminating...");
			        return new Object[]{ 22, new byte[]{} };
		        }, new Object[]{ chunk_number }));
	}

	private void restore() {
		String filename = header[3].toUpperCase() + "." + header[4];

		LinkedTransientQueue<byte[]> replies;
		if (peer.chunks_metadata.containsKey(filename) &&
		    peer.MDRmessages.putIfAbsent(filename, replies = new LinkedTransientQueue<>()) == null) {
			Path pathname = Paths.get("src/dbs/peer/data/" + filename);

			byte[] chunk_body;
			try {
				chunk_body = Files.readAllBytes(pathname);
			}
			catch (IOException e) {
				peer.MDRmessages.remove(filename);
				// File couldn't be read; can't risk sending corrupted files
				System.err.println("\nFAILURE! File could not be read" +
				                   "\nRESTORE protocol terminating...");
				return;
			}
			
			replies.init(ThreadLocalRandom.current().nextInt(401), TimeUnit.MILLISECONDS);
			if(replies.poll() == null) {
				byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.CHUNK, peer.PROTOCOL_VERSION,
				                                                         peer.ID, header[3].toUpperCase(),
				                                                         Integer.parseInt(header[4]), null);
				byte[] chunk = GenericArrays.join(chunk_header, chunk_body);
				
				try {
					peer.MDRsocket.send(chunk);
				}
				catch (IOException e) {
					// Failed sending file; better luck next time
				}
			}
			
			peer.MDRmessages.remove(filename);
		}
	}

	static RemoteFunction delete(Peer peer, String pathname) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	private void delete() {
		// TODO
	}

	static RemoteFunction reclaim(Peer peer, int disk_space) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	private void reclaim() {
		// TODO
	}
}
