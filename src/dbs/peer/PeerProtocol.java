package dbs.peer;

import java.io.File;
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

import dbs.peer.PeerUtility.MessageType;
import dbs.peer.PeerUtility.FileMetadata;
import dbs.peer.PeerUtility.ChunkMetadata;
import dbs.rmi.RemoteFunction;
import dbs.util.GenericArrays;
import dbs.util.concurrent.LinkedTransientQueue;

import static dbs.peer.PeerUtility.MAXIMUM_FILE_SIZE;
import static dbs.peer.PeerUtility.MAXIMUM_CHUNK_SIZE;
import static dbs.peer.PeerUtility.DATA_DIRECTORY;

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
		if (peer.backup_messages.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > MAXIMUM_FILE_SIZE) {
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
		if (peer.files_metadata.containsKey(filename) || peer.local_chunks_metadata.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File already exists in the service metadata" +
				                   "\nBACKUP protocol terminating...");
				return 13;
			});
		}

		LinkedList<String> warnings = new LinkedList<>();

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
		peer.backup_messages.put(fileID, messages);

		int replies;
		int chunk_number = 0;
		int chunk_amount = file.length / MAXIMUM_CHUNK_SIZE + 1;
		peer.files_metadata.put(filename, new FileMetadata(fileID, chunk_amount));
		do {
			HashSet<String> stored_peers = new HashSet<>();

			String chunkID = fileID + "." + chunk_number;
			int putchunk_body_length = (chunk_number + 1) * MAXIMUM_CHUNK_SIZE < file.length ?
			                           (chunk_number + 1) * MAXIMUM_CHUNK_SIZE :
			                           file.length;
			byte[] putchunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                            peer.ID, fileID,
			                                                            chunk_number, replication_degree);
			byte[] putchunk_body = Arrays.copyOfRange(file, chunk_number * MAXIMUM_CHUNK_SIZE, putchunk_body_length);
			byte[] putchunk = GenericArrays.join(putchunk_header, putchunk_body);

			peer.remote_chunks_metadata.put(chunkID, new ChunkMetadata(replication_degree, stored_peers));

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

					if (chunkID.equals(stored_header[3].toUpperCase() + "." + stored_header[4])) {
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

			do {
				peer.remote_chunks_metadata.remove(fileID + "." + chunk_number);
			} while (--chunk_number >= 0);

			if (failed_chunk_number > 0) {
				peer.executor.execute(new PeerProtocol(peer, PeerUtility.generateProtocolHeader(MessageType.DELETE, peer.PROTOCOL_VERSION,
				                                                                                peer.ID, fileID,
				                                                                                null, null)));
			}
		}

		peer.backup_messages.remove(fileID);

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
		String chunkname = header[3].toUpperCase() + "." + header[4];
		Path pathname = Paths.get(DATA_DIRECTORY + chunkname);

		HashSet<String> putchunk_peers = new HashSet<>();
		putchunk_peers.add(peer.ID);

		if (!peer.remote_chunks_metadata.containsKey(chunkname) &&
		    peer.local_chunks_metadata.putIfAbsent(chunkname, new ChunkMetadata(Integer.valueOf(header[5]), putchunk_peers)) == null) {
			try {
				Files.write(pathname, body, StandardOpenOption.CREATE_NEW, StandardOpenOption.DSYNC);
			}
			catch (IOException e) {
				peer.local_chunks_metadata.remove(chunkname);
				// File already exists; can't risk corrupting existing files
				System.err.println("\nFAILURE! File already exists" +
				                   "\nBACKUP protocol terminating...");
				return;
			}
		}

		try {
			int duration = ThreadLocalRandom.current().nextInt(401);
			TimeUnit.MILLISECONDS.sleep(duration);

			byte[] stored = PeerUtility.generateProtocolHeader(MessageType.STORED, peer.PROTOCOL_VERSION,
			                                                   peer.ID, header[3],
			                                                   Integer.valueOf(header[4]), null);
			peer.MCsocket.send(stored);
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

		FileMetadata filemetadata;
		if ((filemetadata = peer.files_metadata.get(filename)) == null) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File does not exist in this service metadata" +
				                   "\nRESTORE protocol terminating...");
				return new Object[]{ 21, new byte[]{} };
			});
		}

		byte[] file = new byte[]{};

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
		peer.restore_messages.put(filemetadata.fileID, messages);

		// Works the same way as backup but it is supposed to be TCP
		byte[] chunk;
		int chunk_number = 0;
		do {
			byte[] getchunk = PeerUtility.generateProtocolHeader(MessageType.GETCHUNK, peer.PROTOCOL_VERSION,
			                                                     peer.ID, filemetadata.fileID,
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

					if (filemetadata.fileID.equals(chunk_header[3].toUpperCase()) && chunk_number == Integer.valueOf(chunk_header[4])) {
						file = GenericArrays.join(file, Arrays.copyOfRange(chunk, chunk_header_length, chunk.length));
						// We got the chunk we want, break from this loop
						break;
					}
				}
			}
		} while (chunk != null && ++chunk_number < filemetadata.chunk_amount);

		peer.restore_messages.remove(filemetadata.fileID);

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
		Path pathname = Paths.get(DATA_DIRECTORY + filename);

		LinkedTransientQueue<byte[]> replies;

		if (peer.local_chunks_metadata.containsKey(filename) &&
		    peer.restore_messages.putIfAbsent(filename, replies = new LinkedTransientQueue<>()) == null) {

			byte[] chunk_body;
			try {
				chunk_body = Files.readAllBytes(pathname);
			}
			catch (IOException e) {
				peer.restore_messages.remove(filename);
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
			
			peer.restore_messages.remove(filename);
		}
	}

	static RemoteFunction delete(Peer peer, String filename) {
		if (peer.instances.getAndIncrement() < 0) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Peer process terminating...");
				return 1;
			});
		}

		FileMetadata filemetadata;
		if ((filemetadata = peer.files_metadata.get(filename)) == null) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File does not exist in this service metadata" +
				                   "\nDELETE protocol terminating...");
				return 31;
			});
		}

		byte[] delete = PeerUtility.generateProtocolHeader(MessageType.DELETE, peer.PROTOCOL_VERSION,
		                                                   peer.ID, filemetadata.fileID,
		                                                   null, null);

		peer.executor.execute(() -> {
			for (int requests = -1; requests < 4; ++requests) {
				try {
					if (requests != -1) {
						TimeUnit.SECONDS.sleep(1 << requests);
					}

					peer.MCsocket.send(delete);
				} catch (IOException | InterruptedException e) {
					// Shouldn't happen
				}
			}

			peer.files_metadata.remove(filename);
		});

		return new RemoteFunction<>((args) -> {
			System.out.println("\nSUCCESS! File deleted");
			return 0;
		});
	}

	private void delete() {
		File[] chunks = new File(DATA_DIRECTORY).listFiles(
				(dir, name) -> name.matches(header[3].toUpperCase() + "\\.[0-9]{1,6}"));
		if (chunks != null) {
			for (File chunk : chunks) {
				chunk.delete();
				peer.local_chunks_metadata.remove(chunk.getName().split("\\.")[0]);
			}
		}
	}

	static RemoteFunction reclaim(Peer peer, int disk_space) {
		// TODO
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	private void reclaim() {
		String chunkname = header[3].toUpperCase() + "." + header[4];
		ChunkMetadata chunkmetadata = peer.local_chunks_metadata.get(chunkname);
		
		if(chunkmetadata.perceived_replication.size() < chunkmetadata.desired_replication) {
			
			Path pathname = Paths.get(DATA_DIRECTORY + chunkname);

			LinkedTransientQueue<byte[]> replies;

			if (peer.reclaim_messages.putIfAbsent(chunkname, replies = new LinkedTransientQueue<>()) == null) {

				byte[] chunk_body;
				
				try {
					chunk_body = Files.readAllBytes(pathname);
				}
				catch (IOException e) {
					peer.reclaim_messages.remove(chunkname);
					// File couldn't be read; can't risk sending corrupted files
					System.err.println("\nFAILURE! File could not be read" +
					                   "\nRECLAIM protocol terminating...");
					return;
				}
				
				replies.init(ThreadLocalRandom.current().nextInt(401), TimeUnit.MILLISECONDS);
				if(replies.poll() == null) {
					byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
					                                                         peer.ID, header[3].toUpperCase(),
					                                                         Integer.parseInt(header[4]), chunkmetadata.desired_replication);
					
					byte[] chunk = GenericArrays.join(chunk_header, chunk_body);
					
					LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
					peer.backup_messages.put(header[3].toUpperCase(), messages);
					
					int number_of_replies = 0;
					int requests = 0;
					while (number_of_replies < chunkmetadata.desired_replication && requests < 5) {
						try {
							peer.MDBsocket.send(chunk);
						}
						catch (IOException e) {
							// Shouldn't happen
						}

						byte[] stored;
						messages.init((1 << requests++), TimeUnit.SECONDS);
						while ((stored = messages.poll()) != null) {
							String[] stored_header = new String(stored).split("[ ]+");

							if (chunkname.equals(stored_header[3].toUpperCase() + "." + stored_header[4])) {
								++number_of_replies;
							}
						}
					}
					
					peer.backup_messages.remove(header[3].toUpperCase());
				}
				
				peer.reclaim_messages.remove(chunkname);
			}
		}
	}
}
