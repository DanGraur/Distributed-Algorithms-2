package node;

import node.message.Message;
import node.remote.CommunicationChannel;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * This is the node of the system
 *
 * @author Dan Graur 11/25/2017
 */
public class Component implements CommunicationChannel, Runnable, Serializable {
    /**
     * The pid of this component
     */
    private long pid;

    /**
     * The name under which the component's stub is published
     */
    private String name;

    /**
     * Reference to the registry object
     */
    private Registry registry;

    /**
     * Map which holds a queue for each of the incoming links
     */
    private Map<String, Queue<Message>> incomingLinks;

    /**
     * Map which hold a queue for the outgoing links
     */
    private Map<String, CommunicationChannel> outgoingLinks;

    /**
     * The node's clock
     */
    private long sClock;

    /**
     * The 'vector clock' which holds the times of the last received/sent message for each of the other processes
     */
    private long[] state;

    /**
     * Class Constructor
     *
     * @param registry a reference to the RMI registry
     * @param numberOfProcesses the total number of nodes in the system
     */
    public Component(String name, long pid, Registry registry, int numberOfProcesses) {
        this.registry = registry;

        this.name = name;
        this.pid = pid;
        this.incomingLinks = new HashMap<>();
        this.outgoingLinks = new HashMap<>();

        this.state = new long[numberOfProcesses - 1];
    }

    /**
     * Find the other peers by looking them up in the registry
     *
     * @throws RemoteException
     * @throws NotBoundException
     */
    private void findPeers() throws RemoteException, NotBoundException {

        for (String stubName : registry.list()) {
            outgoingLinks.put(stubName, (CommunicationChannel) registry.lookup(stubName));

            if (!incomingLinks.containsKey(stubName))
                incomingLinks.put(
                        stubName,
                        new PriorityBlockingQueue<>()
                );
        }
        
    }

    public long getPid() {
        return pid;
    }

    public String getName() {
        return name;
    }

    public long getsClock() {
        return sClock;
    }

    public long[] getState() {
        return state;
    }


    /**
     * This method will be used throguh RMI, hence the confusing name. Other
     * Processes will make use of it in order to send messages to this process
     *
     * @param message the received message
     * @throws RemoteException
     */
    @Override
    public void sendMessage(Message message) throws RemoteException {
        long sourcePid = message.getPid();

        state[(int) sourcePid] = Math.max(message.getsClock(), sClock) + 1;

        incomingLinks.get(message.getProcName()).add(message);
    }

    @Override
    public void run() {

        /* The infinite loop */
        while(true) {


            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("Thread " + name + "Was interrupted whilst waiting");

                e.printStackTrace();
            }
        }

    }
}
