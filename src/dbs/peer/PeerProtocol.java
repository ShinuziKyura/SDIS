package dbs.peer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
		if (peer.instances.getAndIncrement() < 0) {
			return;
		}

		this.peer = peer;
		String header = new String(message).split("\r\n\r\n", 2)[0];
		this.header = header.split("[ ]+");
		byte[] body = GenericArrays.split(message, header.getBytes().length)[1];
		this.body = body.length > 4 ? Arrays.copyOfRange(body, 4, body.length) : null;
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
				System.err.println("\nERROR! Peer process terminating...");
				return 1;
			});
		}
		if (peer.DB_messages.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > PeerUtility.MAXIMUM_FILE_SIZE) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}
		if (peer.stored_files.containsKey(filename) || peer.stored_chunks.containsKey(fileID)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! File already exists in the service data" +
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
			int chunk_end = (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE < file.length ?
			                 (chunk_count + 1) * PeerUtility.MAXIMUM_CHUNK_SIZE :
			                 file.length;
			byte[] message_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, peer.PROTOCOL_VERSION,
			                                                         peer.ID, fileID,
			                                                         chunk_count, replication_degree);
			byte[] message_body = Arrays.copyOfRange(file, chunk_count * PeerUtility.MAXIMUM_CHUNK_SIZE, chunk_end);
			byte[] message = GenericArrays.join(message_header, message_body);

			peer.stored_chunks.put(chunk_name, new AtomicInteger(0));

			stored = 0;
			requests = 0;
			while (stored < replication_degree && requests < 5) {
				byte[] reply;
				try {
					peer.MDBSocket.send(message);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				replies.init((1 << requests++), TimeUnit.SECONDS);
				while ((reply = replies.take()) != null) {
					String[] header = new String(reply).split("[ ]+");
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

		Integer failed_chunk = null;
		if (stored == 0) {
			failed_chunk = chunk_count - 1;
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

		peer.instances.decrementAndGet();

		return (stored != 0 ?
		        new RemoteFunction<>((args) -> {
		        	System.out.println("\nFile successfully backed up");
			        return 0;
		        }) :
		        new RemoteFunction<>((args) -> {
			        System.err.println("\nERROR! Chunk " + args[0] + " could not be stored" +
			                           "\nBACKUP protocol terminating...");
			        return 21;
		        }, new String[]{ failed_chunk.toString() }));
	}

	private void backup() {
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

	public static RemoteFunction restore(Peer peer, String filename) {
		// TODO
		if (peer.instances.getAndIncrement() < 0) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! Peer process terminating...");
				return 1;
			});
		}
		if (!peer.stored_files.containsKey(filename)) {
			peer.instances.decrementAndGet();
			return new RemoteFunction<>((args) -> {
				System.err.println("\nERROR! The file you are trying to restore was not backed up by this peer!" +
						"\nRESTORE protocol terminating...");
				return 2;
			});
		}
		
		String fileID = peer.stored_files.get(filename).toUpperCase();
		
		int chunk_number=0;
		byte[] chunk=null;
		byte[] chunk_body=null;
		
		LinkedTransientQueue<byte[]> replies = new LinkedTransientQueue<>();
		peer.DR_messages.put(fileID, replies);
		
		File file = new File("src/dbs/peer/restored_data/" + filename);
		FileOutputStream file_stream = null;
		
		try {
			file.createNewFile();
			file_stream = new FileOutputStream(file);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		//works the same way as backup but it is supposed to be TCP
		do {
			byte[] chunk_request = PeerUtility.generateProtocolHeader(MessageType.GETCHUNK, peer.PROTOCOL_VERSION,
																								peer.ID, fileID,
																								chunk_number, null);
			int requests = 0;
			boolean received = false;
			
			while (!received && requests < 5) {
				try {
					peer.MCSocket.send(chunk_request);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				replies.init((1 << requests++), TimeUnit.SECONDS);
				while ((chunk = replies.take()) != null) {
					String[] header = new String(chunk).split("\r\n\r\n", 2)[0].split("[ ]+");
					if (header[3].toUpperCase().equals(fileID) && Integer.parseInt(header[4])==chunk_number) {
						
						chunk_body = GenericArrays.split(chunk, new String(chunk).split("\r\n\r\n", 2)[0].getBytes().length)[1];
						chunk_body = chunk_body.length > 4 ? java.util.Arrays.copyOfRange(chunk_body, 4, chunk_body.length) : null;
						
						if(chunk_body != null) {
							try {
								file_stream.write(chunk_body);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						received=true;
						break;
					}
				}
			}
			
			chunk_number++;
		
		}while(chunk_body != null && chunk_body.length == PeerUtility.MAXIMUM_CHUNK_SIZE);

		peer.DR_messages.remove(fileID);
		
		try {
			file_stream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Successfully Restored File!");
		
		return new RemoteFunction<>((args) -> {
			return 0;
		});
	}

	public void restore() {
		String filename = header[3].toUpperCase() + "." + header[4];
		if (peer.stored_chunks.containsKey(filename) && !peer.DR_messages.containsKey(filename)) {
			Path filepath = Paths.get("src/dbs/peer/data/" + filename);

			byte[] chunk_body=null;
			
			try {
				chunk_body = Files.readAllBytes(filepath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			LinkedTransientQueue<byte[]> replies = new LinkedTransientQueue<>();
			peer.DR_messages.put(filename, replies);
			
			replies.init(ThreadLocalRandom.current().nextInt(401), TimeUnit.MILLISECONDS);
			
			if(replies.take() == null) {
				byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.CHUNK, peer.PROTOCOL_VERSION,
																									peer.ID, header[3].toUpperCase(),
																									Integer.parseInt(header[4]), null);
				byte[] chunk = GenericArrays.join(chunk_header, chunk_body);
				
				try {
					peer.MDRSocket.send(chunk);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			peer.DR_messages.remove(filename);
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
