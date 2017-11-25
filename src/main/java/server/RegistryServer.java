package server;

import node.Node;
import node.remote.CommunicationChannel;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author Dan Graur 11/25/2017
 */
public class RegistryServer {

    public static void main(String[] args) throws RemoteException {

        System.setProperty("java.security.policy", "file:./src/main/resources/generic.policy");
        System.setSecurityManager(new SecurityManager());

        Registry registry = LocateRegistry.createRegistry(1099);

        CommunicationChannel baseObject = new Node();
        CommunicationChannel baseStub = (CommunicationChannel) UnicastRemoteObject.exportObject(baseObject, 60001);

        registry.rebind("DUMMY_928301928", baseStub);

        for (String s : registry.list()) {
            System.out.println(s);
        }
    }
}
