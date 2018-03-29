package dbs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    void backup(String filename, String fileID, byte[] file, int replication_degree) throws RemoteException;

    void restore(String filename) throws RemoteException;

    void delete(String filename) throws RemoteException;

    void reclaim(int disk_space) throws RemoteException;

    String state() throws RemoteException;

    void stop() throws RemoteException;
}
