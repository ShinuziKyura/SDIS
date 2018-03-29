package dbs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    int backup(String filename, String fileID, byte[] file, int replication_degree) throws RemoteException;

    int restore(String filename) throws RemoteException;

    int delete(String filename) throws RemoteException;

    int reclaim(int disk_space) throws RemoteException;

    String state() throws RemoteException;

    int stop() throws RemoteException;

    int stop(long duration) throws RemoteException;
}
