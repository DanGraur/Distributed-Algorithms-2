package server;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @author Dan Graur 11/25/2017
 */
public class RegistryServer {

    public static void main(String[] args) throws IOException {

        System.setProperty("java.security.policy", "file:./src/main/resources/generic.policy");
        System.setSecurityManager(new SecurityManager());

        Registry registry = LocateRegistry.createRegistry(1099);

        /*CommunicationChannel baseObject = new Component();
        CommunicationChannel baseStub = (CommunicationChannel) UnicastRemoteObject.exportObject(baseObject, 60001);

        registry.rebind("DUMMY_928301928", baseStub);

        for (String s : registry.list()) {
            System.out.println(s);
        }*/

        System.in.read();
    }
}
