package dbs.peer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// Collection of utilities used throughout our code
public class PeerUtility {
    public static final String PROTOCOL_VERSION_REGEX =
            "[0-9]\\.[0-9]";
    public static final String PEER_ID_REGEX =
            "[1-9][0-9]{0,8}|0";
    public static final String ACCESS_POINT_REGEX =
            "[ !-.0-~]+";
    public static final String ADDRESS_REGEX =
            "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\." +
            "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])";
    public static final String PORT_REGEX =
            "6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{0,3}|0";

	public static final long MAXIMUM_FILE_SIZE = 63999999999L;
	public static final int MAXIMUM_CHUNK_SIZE = 64000;

	public static final MessageDigest SHA_256_HASHER = SHA_256_HASHER();
    private static MessageDigest SHA_256_HASHER() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
	        System.err.println("\nERROR! Could not find instance of SHA-256 in system" +
	                           "\nDistributed Backup Service terminating...");
            System.exit(1); // The Uh-Oh-That-Cant-Be-Good status code
        }
        return null;
    }

	public enum MessageType {
        PUTCHUNK,
        STORED,
        GETCHUNK,
        CHUNK,
        DELETE,
        REMOVED
    }

	public static class ProtocolVersion {
		public final Integer MAJOR_NUMBER;
		public final Integer MINOR_NUMBER;

		public ProtocolVersion(String version) {
			MAJOR_NUMBER = Character.getNumericValue(version.charAt(0));
			MINOR_NUMBER = Character.getNumericValue(version.charAt(2));
		}

		@Override
		public String toString() {
			return MAJOR_NUMBER + "." + MINOR_NUMBER;
		}
	}

    public static byte[] generateProtocolHeader(MessageType message_type, ProtocolVersion protocol_version,
                                                String id, String fileID,
                                                Integer chunk_number, Integer replication_degree) {
        String header = message_type + " " + protocol_version + " " + id + " " + fileID + " ";

        switch (message_type) {
            case PUTCHUNK:
                header += chunk_number + " " + replication_degree + " \r\n\r\n";
                break;
            case STORED:
            case GETCHUNK:
            case CHUNK:
            case REMOVED:
                header += chunk_number + " \r\n\r\n";
                break;
        }

        return header.getBytes();
    }

    public static String generateFileID(Path filepath) throws IOException {
        BasicFileAttributes filemetadata = Files.readAttributes(filepath, BasicFileAttributes.class);
        String filename = "N" + filepath.getFileName() +
                          "S" + filemetadata.size() +
                          "C" + filemetadata.creationTime() +
                          "M" + filemetadata.lastModifiedTime() +
                          "A" + filemetadata.lastAccessTime();


        byte[] hash = SHA_256_HASHER.digest(filename.getBytes());

        StringBuilder hash_string = new StringBuilder(64);

        for (byte hash_byte : hash) {
            hash_string.append(String.format("%02x", hash_byte));
        }

        return hash_string.toString().toUpperCase();
    }
}
