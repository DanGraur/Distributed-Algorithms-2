package node.remote;

import node.message.Message;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Dan Graur 11/25/2017
 */
public interface CommunicationChannel extends Remote {

    void sendMessage(Message message) throws RemoteException;

}
