package dbs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PeerProtocol {

	
	private String hash(String text) throws NoSuchAlgorithmException {
		MessageDigest digest;
		digest = MessageDigest.getInstance("SHA-256");
		
		String hash_string="";
		byte[] hash = digest.digest(text.getBytes());
		
		for(int i=0; i < hash.length; i++) {
			hash_string=hash_string + String.format("%02x", hash[i]);
		}

		return hash_string;
	}
}



/*
at buffer[0]:
MessageType: 8 (trailing whitespace when its less than 8 characters)
whitespace
at buffer[9]:
Version: 3
whitespace
at buffer[13]:
SenderID: 10 (trailing whitespace when it's less than 10 characters - 10 characters cause INTMAX == 2^32 has ten digits at most)
whitespace
at buffer[24]:
FileID: 32
whitespace
at buffer[57]:
ChunkNo: 6
whitespace
at buffer[64]:
ReplicationDeg: 1
whitespace
at buffer[66]:
CRLF: 2
CRLF: 2
at buffer[70]:
Body: at most 64000
*/