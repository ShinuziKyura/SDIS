package dbs.peer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
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

	PeerProtocol(Peer peer, byte[] message) {
		this.peer = peer;

		String header = new String(message).split("\r\n\r\n", 2)[0];
		this.header = header.split("[ ]+");

		byte[] body = GenericArrays.split(message, header.getBytes().length)[1];
		this.body = body.length > 4 ? Arrays.copyOfRange(body, 4, body.length) : new byte[]{};
	}

	@Override
	public void run() {
		switch (header[0].toUpperCase()) {
			case "PUTCHUNK":
				peer.shared_access.lock();
				backup();
				peer.shared_access.unlock();
				break;
			case "GETCHUNK":
				peer.shared_access.lock();
				restore();
				peer.shared_access.unlock();
				break;
			case "DELETE":
				peer.exclusive_access.lock();
				delete();
				peer.exclusive_access.unlock();
				break;
			case "REMOVED":
				peer.shared_access.lock();
				reclaim();
				peer.shared_access.unlock();
				break;
		}
	}

	static RemoteFunction backup(Peer peer, String filename, String fileID, byte[] file, int replication_degree) {
		if (file.length == 0 || file.length > MAXIMUM_FILE_SIZE) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}
		if (peer.files_metadata.containsKey(filename)) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File already exists in the service metadata" +
				                   "\nBACKUP protocol terminating...");
				return 13;
			});
		}

		LinkedList<String> warnings = new LinkedList<>();
		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();
		LinkedHashMap<String, Set<String>> chunks_stored_peers = new LinkedHashMap<>();

		if (peer.backup_messages.putIfAbsent(fileID, messages) != null) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Backup protocol already running for this file" +
				                   "\nBACKUP protocol terminating...");
				return 14;
			});
		}

		int chunk_size;
		int chunk_number = 0;
		int chunk_amount = file.length / MAXIMUM_CHUNK_SIZE + 1;
		do {
			String chunkID = fileID + "." + chunk_number;
			chunk_size = (chunk_number + 1) * MAXIMUM_CHUNK_SIZE < file.length ?
			             (chunk_number + 1) * MAXIMUM_CHUNK_SIZE :
			             file.length;
			byte[] putchunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                            peer.ID, fileID,
			                                                            chunk_number, replication_degree);
			byte[] putchunk_body = Arrays.copyOfRange(file, chunk_number * MAXIMUM_CHUNK_SIZE, chunk_size);
			byte[] putchunk = GenericArrays.join(putchunk_header, putchunk_body);

			Set<String> stored_peers = ConcurrentHashMap.newKeySet();

			int requests = 0;
			while (stored_peers.size() < replication_degree && requests < 5) {
				peer.log.print("\nBackup -> Sending PUTCHUNK message:" +
					               "\n\tChunk: " + chunkID +
					               "\n\tAttempt: " + (requests + 1));

				peer.MDBsender.send(putchunk);

				byte[] stored;
				messages.clear((1 << requests++), TimeUnit.SECONDS);
				while ((stored = messages.poll()) != null) {
					String[] stored_header = new String(stored).split("[ ]+");

					if (chunkID.equals(stored_header[3].toUpperCase() + "." + stored_header[4]) &&
					    stored_peers.add(stored_header[2])) {
						peer.log.print("\nBackup -> Received STORED message:" +
						               "\n\tSender: " + stored_header[2] +
						               "\n\tChunk: " + chunkID);
					}
				}
			}
			if (stored_peers.size() == 0) {
				break;
			}
			else if (stored_peers.size() < replication_degree) {
				warnings.add("\nWARNING! Replication degree of chunk " + chunk_number + " may be lower than requested:" +
				             "\n\tDesired - " + replication_degree +
				             "\n\tPerceived - " + stored_peers.size());
			}

			chunks_stored_peers.put(chunkID, stored_peers);
		} while (++chunk_number < chunk_amount);

		peer.backup_messages.remove(fileID);

		if (chunk_number != chunk_amount) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Chunk " + args[0] + " could not be stored" +
				                   "\nBACKUP protocol terminating...");
				return 15;
			}, new Object[]{ chunk_number });
		}

		peer.files_metadata.put(filename, new FileMetadata(fileID, chunk_amount, replication_degree));
		for (Map.Entry<String, Set<String>> csp : chunks_stored_peers.entrySet()) {
			peer.remote_chunks_metadata.put(csp.getKey(), new ChunkMetadata(
					!csp.getKey().split("\\.")[1].equals(chunk_amount - 1) ? MAXIMUM_CHUNK_SIZE : chunk_size,
					replication_degree,
					csp.getValue()));
		}

		return new RemoteFunction<>((args) -> {
			for (String arg : (LinkedList<String>) args[0]) {
				System.out.println(arg);
			}
			System.out.println("\nSUCCESS! File stored");
			return 0;
		}, new Object[]{ warnings });
	}

	private void backup() {
		String chunkID = header[3].toUpperCase() + "." + header[4];

		Set<String> putchunk_peers = ConcurrentHashMap.newKeySet();
		putchunk_peers.add(peer.ID);

		if (!peer.remote_chunks_metadata.containsKey(chunkID)) {
			if (peer.storage_usage.addAndGet(body.length) <= peer.storage_capacity.get()) {
				peer.log.print("\nBackup <- Received PUTCHUNK message:" +
				               "\n\tSender: " + header[2] +
				               "\n\tChunk: " + chunkID);

				byte[] stored = PeerUtility.generateProtocolHeader(MessageType.STORED, peer.PROTOCOL_VERSION,
				                                                   peer.ID, header[3],
				                                                   Integer.valueOf(header[4]), null);

				if (peer.local_chunks_metadata.putIfAbsent(chunkID, new ChunkMetadata(body.length, Integer.valueOf(header[5]), putchunk_peers)) == null) {
					try {
						Files.write(Paths.get(DATA_DIRECTORY + chunkID), body,
						            StandardOpenOption.CREATE_NEW, StandardOpenOption.DSYNC);

					} catch (IOException e) {
						// Really shouldn't happen, we won't delete the file from the local_chunks_metadata
						peer.storage_usage.addAndGet(-body.length);
						return;
					}
				}

				try {
					TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(401));
				}
				catch (InterruptedException e) {
					// Shouldn't happen
				}

				peer.log.print("\nBackup <- Sending STORED message:" +
				               "\n\tChunk: " + chunkID);

				peer.MCsender.send(stored);
			}
			else {
				peer.storage_usage.addAndGet(-body.length);
			}
		}
	}

	static RemoteFunction restore(Peer peer, String filename) {
		FileMetadata filemetadata;
		if ((filemetadata = peer.files_metadata.get(filename)) == null) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File does not exist in this service metadata" +
				                   "\nRESTORE protocol terminating...");
				return 21;
			});
		}
		if (peer.backup_messages.containsKey(filemetadata.fileID)) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File is being backed up by this service" +
				                   "\nRESTORE protocol terminating...");
				return 22;
			});
		}

		byte[] file = new byte[]{};
		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();

		peer.restore_messages.put(filemetadata.fileID, messages);

		byte[] chunk;
		int chunk_number = 0;
		do {
			byte[] getchunk = PeerUtility.generateProtocolHeader(MessageType.GETCHUNK, peer.PROTOCOL_VERSION,
			                                                     peer.ID, filemetadata.fileID,
			                                                     chunk_number, null);

			chunk = null;
			int requests = 0;
			while (chunk == null && requests < 5) {
				peer.log.print("\nRestore -> Sending GETCHUNK message:" +
					               "\n\tChunk: " + filemetadata.fileID + "." + chunk_number +
					               "\n\tAttempt: " + (requests + 1));

				peer.MCsender.send(getchunk);

				messages.clear((1 << requests++), TimeUnit.SECONDS);
				while ((chunk = messages.poll()) != null) {
					String[] chunk_header = new String(chunk).split("\r\n\r\n", 2);
					int chunk_header_length = chunk_header[0].length() + 4;
					chunk_header = chunk_header[0].split("[ ]+");

					if (filemetadata.fileID.equals(chunk_header[3].toUpperCase()) && chunk_number == Integer.valueOf(chunk_header[4])) {
						peer.log.print("\nRestore -> Received CHUNK message:" +
						               "\n\tSender: " + chunk_header[2] +
						               "\n\tChunk: " + filemetadata.fileID + "." + chunk_number);

						file = GenericArrays.join(file, Arrays.copyOfRange(chunk, chunk_header_length, chunk.length));
						break;
					}
				}
			}
		} while (chunk != null && ++chunk_number < filemetadata.chunk_amount);

		peer.restore_messages.remove(filemetadata.fileID);

		if (chunk_number != filemetadata.chunk_amount) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Chunk " + args[0] + " could not be restored" +
				                   "\nRESTORE protocol terminating...");
				return 22;
			}, new Object[]{ chunk_number });
		}

		return new RemoteFunction<>((args) -> {
			System.out.println("\nSUCCESS! File restored");
			return (byte[]) args[0];
		}, new Object[] { file });
	}

	private void restore() {
		String chunkID = header[3].toUpperCase() + "." + header[4];
		Path pathname = Paths.get(DATA_DIRECTORY + chunkID);

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();

		if (peer.local_chunks_metadata.containsKey(chunkID) && peer.restore_messages.putIfAbsent(chunkID, messages) == null) {
			try {
				peer.log.print("\nRestore <- Received GETCHUNK message:" +
				               "\n\tSender: " + header[2] +
				               "\n\tChunk: " + chunkID);

				byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.CHUNK, peer.PROTOCOL_VERSION,
				                                                         peer.ID, header[3].toUpperCase(),
				                                                         Integer.valueOf(header[4]), null);
				byte[] chunk_body = Files.readAllBytes(pathname);
				byte[] chunk = GenericArrays.join(chunk_header, chunk_body);

				messages.clear(ThreadLocalRandom.current().nextInt(401), TimeUnit.MILLISECONDS);
				if(messages.poll() == null) {
					peer.log.print("\nRestore <- Sending CHUNK message:" +
					               "\n\tChunk: " + chunkID);

					peer.MDRsender.send(chunk);
				}
			}
			catch (IOException e) {
				// File couldn't be read; can't risk sending corrupted files
			}
			peer.restore_messages.remove(chunkID);
		}
	}

	static RemoteFunction delete(Peer peer, String filename) {
		if (!peer.files_metadata.containsKey(filename)) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! File does not exist in this service metadata" +
				                   "\nDELETE protocol terminating...");
				return 31;
			});
		}

		peer.executor.execute(() -> {
			peer.exclusive_access.lock();

			FileMetadata filemetadata;
			if ((filemetadata = peer.files_metadata.remove(filename)) != null) {
				for (int chunk_number = 0; chunk_number < filemetadata.chunk_amount; ++chunk_number) {
					peer.remote_chunks_metadata.remove(filemetadata.fileID + "." + chunk_number);
				}

				byte[] delete = PeerUtility.generateProtocolHeader(MessageType.DELETE, peer.PROTOCOL_VERSION,
				                                                   peer.ID, filemetadata.fileID,
				                                                   null, null);

				for (int requests = -1; requests < 4; ++requests) {
					if (requests != -1) {
						try {
							TimeUnit.SECONDS.sleep(1 << requests);
						}
						catch (InterruptedException e) {
							// Shouldn't happen
						}
					}

					peer.log.print("\nDelete -> Sending DELETE message:" +
					               "\n\tFile: " + filemetadata.fileID +
					               "\n\tAttempt: " + (requests + 2));

					peer.MCsender.send(delete);
				}
			}

			peer.exclusive_access.unlock();
		});

		return new RemoteFunction<>((args) -> {
			System.out.println("\nSUCCESS! File being deleted");
			return 0;
		});
	}

	private void delete() {
		File[] chunks = new File(DATA_DIRECTORY).listFiles(
				(dir, name) -> name.matches(header[3].toUpperCase() + "\\.([1-9][0-9]{0,5}|0)"));

		if (chunks != null) {
			peer.log.print("\nDelete <- Received DELETE message:" +
			               "\n\tSender: " + header[2] +
			               "\n\tFile: " + header[3].toUpperCase());

			for (File chunk : chunks) {
				if (peer.local_chunks_metadata.remove(chunk.getName()) != null) {
					peer.storage_usage.addAndGet(-chunk.length());
					chunk.delete();
				}
			}
		}
	}

	static RemoteFunction reclaim(Peer peer, long disk_space) {
		File[] chunks = new File(DATA_DIRECTORY).listFiles(
				(dir, name) -> name.matches("[0-9A-F]{64}\\.([1-9][0-9]{0,5}|0)"));

		peer.storage_capacity.set((disk_space *= 1000) != 0 ? disk_space : Long.MAX_VALUE);

		if (chunks == null) {
			return new RemoteFunction<>((args) -> {
				System.err.println("\nFAILURE! Could not reclaim disk space" +
				                   "\nRECLAIM protocol terminating...");
				return 41;
			});
		}

		peer.executor.execute(() -> {
			peer.exclusive_access.lock();

			if (peer.storage_capacity.get() == Long.MAX_VALUE) {
				for (File chunk : chunks) {
					if (peer.local_chunks_metadata.remove(chunk.getName()) != null) {
						peer.storage_usage.addAndGet(-chunk.length());
						
						chunk.delete();

						String[] chunkmetadata = chunk.getName().split("\\.");

						byte[] removed = PeerUtility.generateProtocolHeader(MessageType.REMOVED, peer.PROTOCOL_VERSION,
						                                                    peer.ID, chunkmetadata[0],
						                                                    Integer.valueOf(chunkmetadata[1]), null);

						peer.log.print("\nReclaim -> Sending REMOVED message:" +
						               "\n\tChunk: " + chunk.getName());

						peer.MCsender.send(removed);
					}
				}
			}
			else {
				String[] chunkIDs = peer.local_chunks_metadata.keySet().toArray(new String[peer.local_chunks_metadata.size()]);
				Arrays.sort(chunkIDs, (s1, s2) -> {
					ChunkMetadata c1 = peer.local_chunks_metadata.get(s1);
					ChunkMetadata c2 = peer.local_chunks_metadata.get(s2);
					Integer value = Integer.compare(c2.perceived_replication.size() - c2.desired_replication,
					                                c1.perceived_replication.size() - c1.desired_replication);
					return (value != 0 ? value : Integer.compare(c2.chunk_size, c1.chunk_size));
				});

				for (int n = 0; peer.storage_capacity.get() < peer.storage_usage.get() && n < chunkIDs.length; ++n) {
					if (peer.local_chunks_metadata.remove(chunkIDs[n]) != null) {
						File chunk = new File(DATA_DIRECTORY + chunkIDs[n]);

						peer.storage_usage.addAndGet(-chunk.length());

						chunk.delete();

						String[] chunkmetadata = chunkIDs[n].split("\\.");

						byte[] removed = PeerUtility.generateProtocolHeader(MessageType.REMOVED, peer.PROTOCOL_VERSION,
						                                                    peer.ID, chunkmetadata[0],
						                                                    Integer.valueOf(chunkmetadata[1]), null);

						peer.log.print("\nReclaim -> Sending REMOVED message:" +
						               "\n\tChunk: " + chunkIDs[n]);

						peer.MCsender.send(removed);
					}
				}
			}

			peer.exclusive_access.unlock();
		});

		return new RemoteFunction<>((args) -> {
			System.out.println("SUCCESS! Disk space being reclaimed");
			return 0;
		});
	}

	private void reclaim() {
		String chunkID = header[3].toUpperCase() + "." + header[4];
		Path pathname = Paths.get(DATA_DIRECTORY + chunkID);

		LinkedTransientQueue<byte[]> messages = new LinkedTransientQueue<>();

		ChunkMetadata chunkmetadata = peer.local_chunks_metadata.get(chunkID);
		if (chunkmetadata != null && chunkmetadata.desired_replication > chunkmetadata.perceived_replication.size() &&
		    peer.reclaim_messages.putIfAbsent(chunkID, messages) == null) {
			try {
				peer.log.print("\nReclaim <- Received REMOVED message:" +
				               "\n\tSender: " + header[2] +
				               "\n\tChunk: " + chunkID);

				byte[] putchunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
				                                                            peer.ID, header[3].toUpperCase(),
				                                                            Integer.valueOf(header[4]), chunkmetadata.desired_replication);
				byte[] putchunk_body = Files.readAllBytes(pathname);
				byte[] putchunk = GenericArrays.join(putchunk_header, putchunk_body);

				messages.clear(ThreadLocalRandom.current().nextInt(401), TimeUnit.MILLISECONDS);
				if (messages.poll() == null) {
					int requests = 0;
					while (chunkmetadata.perceived_replication.size() < chunkmetadata.desired_replication && requests < 5) {
						peer.log.print("\nReclaim <- Sending PUTCHUNK message:" +
						               "\n\tChunk: " + chunkID +
						               "\n\tAttempt: " + (requests + 1));

						peer.MDBsender.send(putchunk);

						HashSet<String> stored_peers = new HashSet<>();

						byte[] stored;
						messages.clear((1 << requests++), TimeUnit.SECONDS);
						while ((stored = messages.poll()) != null) {
							String[] stored_header = new String(stored).split("[ ]+");

							if (stored_header[0].equals("STORED") &&
							    chunkID.equals(stored_header[3].toUpperCase() + "." + stored_header[4]) &&
							    (chunkmetadata.perceived_replication.add(stored_header[2]) || stored_peers.add(stored_header[2]))) {
								peer.log.print("\nReclaim <- Received STORED message:" +
								               "\n\tSender: " + header[2] +
								               "\n\tChunk: " + chunkID);
							}
						}
					}
				}
				else {
					peer.log.print("\nReclaim <- Received PUTCHUNK message:" +
					               "\n\tSender: " + header[2] +
					               "\n\tChunk: " + chunkID);
				}
			}
			catch (IOException e) {
				// File couldn't be read; can't risk sending corrupted files
			}

			peer.reclaim_messages.remove(chunkID);
		}
	}

	static RemoteFunction failure() {
		return new RemoteFunction<>((args) -> {
			System.err.println("\nFAILURE! Peer process is stopping" +
			                   "\nDistributed Backup Service terminating...");
			return 1;
		});
	}
}
