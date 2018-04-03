package dbs.peer;

import dbs.rmi.RemoteFunction;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    void stop() throws RemoteException;

    String state() throws RemoteException;

    RemoteFunction backup(String filename, String fileID, byte[] file, int replication_degree) throws RemoteException;

	RemoteFunction backup_enhanced(String filename, String fileID, byte[] file, int replication_degree) throws RemoteException;

    RemoteFunction restore(String filename) throws RemoteException;

    RemoteFunction restore_enhanced(String filename) throws RemoteException;

    RemoteFunction delete(String filename) throws RemoteException;

    RemoteFunction reclaim(long disk_space) throws RemoteException;
}
