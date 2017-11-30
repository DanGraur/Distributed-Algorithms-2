import node.Component;
import node.remote.CommunicationChannel;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Dan Graur 11/25/2017
 */
public class Main {

    /**
     * The main method, which initiates the system, using threads
     *
     * args[0] - IP of the registry
     * args[1] - Port of the registry
     * args[2] - Number of threads/components
     * args[3] - Starting port
     */
    public static void main(String[] args) throws RemoteException {

        String registryIp = args[0];
        int registryPort = Integer.parseInt(args[1]);

        int numberOfComponents = Integer.parseInt(args[2]);

        int startingPort = Integer.parseInt(args[3]);

        List<Thread> componentThreads = new ArrayList<>();

        /* Gradually create new threads */
        for (int i = 0; i < numberOfComponents; ++i) {
            Registry registryReference = LocateRegistry.getRegistry(registryIp, registryPort);

            String registryReferenceName = String.valueOf(i);

            /* Create the component and its stub */
            Component component = new Component(registryReferenceName, i, registryReference, numberOfComponents);
            CommunicationChannel stub = (CommunicationChannel) UnicastRemoteObject.exportObject(component, startingPort + i);

            /* Add the stub to the registry */
            registryReference.rebind(registryReferenceName, stub);

            /* Create the thread and run it */
            Thread componentThread = new Thread(component);

            /* Add it to the list of threads */
            componentThreads.add(componentThread);
        }

        for (Thread componentThread : componentThreads) {
            componentThread.start();
        }


        /* Once all of the references have been created join them */
        for (Thread componentThread : componentThreads)
            try {
                componentThread.join();

                System.out.println("A thread ended " + componentThread.getName());
            } catch (InterruptedException e) {
                System.err.println("Was interrupted whilst awaiting for a thread to terminate");

                e.printStackTrace();
            }


    }
}
