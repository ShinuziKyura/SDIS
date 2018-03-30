package dbs;

import rmi.RMIResult;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    void stop() throws RemoteException;

    String state() throws RemoteException;

    RMIResult backup(String filename, String fileID, byte[] file, int replication_degree) throws RemoteException;

    RMIResult restore(String filename) throws RemoteException;

    RMIResult delete(String filename) throws RemoteException;

    RMIResult reclaim(int disk_space) throws RemoteException;
}
