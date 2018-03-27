package dbs;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class PeerChannel implements Runnable {
	private Peer peer;
	private MulticastConnection socket;
	private AtomicBoolean running;
	
	public PeerChannel(Peer peer, MulticastConnection socket) {
		this.peer = peer;
		this.socket = socket;
		this.running = new AtomicBoolean(true);
	}
	
	@Override
	public void run() {
		while (running.get()) {
			try {
				peer.executor.execute(new PeerProtocol(socket.receive()));
			}
			catch (IOException e) {
				// Socket closed, probably terminating,
				// but we'll keep trying to receive() until we are explicitly told to stop
			}
		}
	}
	
	public void stop() {
		this.running.set(false);
	}
}

/*
	Header
	
	The header consists of a sequence of ASCII lines, sequences of ASCII codes terminated with the sequence '0xD''0xA', which we denote <CRLF> because these are the ASCII codes of the CR and LF chars respectively.
	Each header line is a sequence of fields, sequences of ASCII codes, separated by spaces, the ASCII char ' '. Note that:
	
	- there may be more than one space between fields;
	- there may be zero or more spaces after the last field in a line;
	- the header always terminates with an empty header line. I.e. the <CRLF> of the last header line is followed immediately by another <CRLF>, white spaces included, without any character in between.
	
	In the version described herein, the header has only the following non-empty single line:
	
		<MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
	
	Some of these fields may not be used by some messages, but all fields that appear in a message must appear in the relative order specified above.
	
	Next we describe the meaning of each field and its format.
	
	<MessageType>
		This is the type of the message. Each subprotocol specifies its own message types. This field determines the format of the message and what actions its receivers should perform. This is encoded as a variable length sequence of ASCII characters.
	<Version>
		This is the version of the protocol. It is a three ASCII char sequence with the format <n>'.'<m>, where <n> and <m> are the ASCII codes of digits. For example, version 1.0, the one specified in this document, should be encoded as the char sequence '1''.''0'.
	<SenderId>
		This is the id of the server that has sent the message. This field is useful in many subprotocols. This is encoded as a variable length sequence of ASCII digits.
	<FileId>
		This is the file identifier for the backup service. As stated above, it is supposed to be obtained by using the SHA256 cryptographic hash function. As its name indicates its length is 256 bit, i.e. 32 bytes, and should be encoded as a 64 ASCII character sequence.
		The encoding is as follows: each byte of the hash value is encoded by the two ASCII characters corresponding to the hexadecimal representation of that byte. E.g., a byte with value 0xB2 should be represented by the two char sequence 'B''2' (or 'b''2', it does not matter).
		The entire hash is represented in big-endian order, i.e. from the MSB (byte 31) to the LSB (byte 0).
	<ChunkNo>
		This field together with the FileId specifies a chunk in the file. The chunk numbers are integers and should be assigned sequentially starting at 0. It is encoded as a sequence of ASCII characters corresponding to the decimal representation of that number, with the most significant digit first.
		The length of this field is variable, but should not be larger than 6 chars. Therefore, each file can have at most one million chunks. Given that each chunk is 64 KByte, this limits the size of the files to backup to 64 GByte.
	<ReplicationDeg>
		This field contains the desired replication degree of the chunk. This is a digit, thus allowing a replication degree of up to 9. It takes one byte, which is the ASCII code of that digit.
*/

