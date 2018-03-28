package dbs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PeerInterface extends Remote {
    void backup(String pathname, int replication_degree) throws RemoteException, IOException;

    void restore(String pathname) throws RemoteException;

    void delete(String pathname) throws RemoteException;

    void reclaim(int disk_space) throws RemoteException;

    String state() throws RemoteException;
}
