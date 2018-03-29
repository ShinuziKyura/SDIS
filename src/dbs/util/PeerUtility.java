package dbs.util;

import java.lang.reflect.Array;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Enumeration;

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
    public static MessageDigest SHA_256_HASHER = null;
    static {
        try {
            SHA_256_HASHER = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            System.exit(1); // The uh-oh-that-cant-be-good status code
        }
    }

    public static class Version {
        public final int MAJOR_NUMBER;
        public final int MINOR_NUMBER;

        public Version(int major_number, int minor_number) {
            MAJOR_NUMBER = major_number;
            MINOR_NUMBER = minor_number;
        }

        public Version(String version) {
            MAJOR_NUMBER = Character.getNumericValue(version.charAt(0));
            MINOR_NUMBER = Character.getNumericValue(version.charAt(2));
        }

        @Override
        public String toString() {
            return MAJOR_NUMBER + "." + MINOR_NUMBER;
        }
    }

    public enum MessageType {
        PUTCHUNK,
        STORED,
        GETCHUNK,
        CHUNK,
        DELETE,
        REMOVED,
        STOP
    }

    public static byte[] generateProtocolHeader(MessageType message_type, Version protocol_version,
                                                String id, String fileID,
                                                Integer chunk_number, Integer replication_degree) {
        String header = message_type + " " +
                        protocol_version + " " +
                        id + " " +
                        fileID + " ";

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

        return hash_string.toString();
    }

    // Code adapted from here: https://stackoverflow.com/a/80503
    public static <T> T mergeArrays(T first, T second) throws IllegalArgumentException {
        if (!first.getClass().isArray() || !second.getClass().isArray()) {
            throw new IllegalArgumentException();
        }

        Class<?> result_type;
        Class<?> first_type = first.getClass().getComponentType();
        Class<?> second_type = second.getClass().getComponentType();

        if (first_type.isAssignableFrom(second_type)) {
            result_type = first_type;
        }
        else if (second_type.isAssignableFrom(first_type)) {
            result_type = second_type;
        }
        else {
            throw new IllegalArgumentException();
        }

        int first_length = Array.getLength(first);
        int second_length = Array.getLength(second);

        @SuppressWarnings("unchecked")
        T result = (T) Array.newInstance(result_type, first_length + second_length);
        System.arraycopy(first, 0, result, 0, first_length);
        System.arraycopy(second, 0, result, first_length, second_length);

        return result;
    }

    // Code adapted from here: https://stackoverflow.com/a/8462548
    public static NetworkInterface testInterfaces() throws IOException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface netInt : Collections.list(interfaces)) {
            // we don't care about loopback addresses
            // or interfaces that aren't up and running
            if (netInt.isLoopback() || !netInt.isUp()) {
                continue;
            }
            // iterate over the addresses associated with the interface
            Enumeration<InetAddress> addresses = netInt.getInetAddresses();
            for (InetAddress InetAddr : Collections.list(addresses)) {
                // we look only for ipv4 addresses
                // and use a timeout big enough for our needs
                if (InetAddr instanceof Inet6Address || !InetAddr.isReachable(1000)) {
                    continue;
                }
                // java 7's try-with-resources statement, so that
                // we close the socket immediately after use
                try (SocketChannel socket = SocketChannel.open()) {
                    // bind the socket to our local interface
                    socket.bind(new InetSocketAddress(InetAddr, 0));

                    // try to connect to *somewhere*
                    socket.connect(new InetSocketAddress("example.com", 80));
                }
                catch (IOException e) {
                    continue;
                }

                // stops at the first *working* solution
                return netInt;
            }
        }
        return null;
    }
}
