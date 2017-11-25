package node;

import node.message.Message;
import node.remote.CommunicationChannel;

import java.rmi.RemoteException;

/**
 * @author Dan Graur 11/25/2017
 */
public class Node implements CommunicationChannel {
    @Override
    public void sendMessage(Message message) throws RemoteException {

    }
}
