package node;

import node.message.Message;
import node.message.MessageType;
import node.remote.CommunicationChannel;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
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
     * This queue is used as an initial buffer at the process' input, in order to avoid heavy computation in the sendMessage method
     */
    private List<Message> separatingQueue;

    /**
     * The node's clock; used for assigning the id of the outgoing messages
     */
    private long sClock;

    /**
     * The 'vector clock' which holds the times of the last received/sent message for each of the other processes
     */
    private long[] state;

    /**
     * Indicates if the local state is being recorded
     */
    private boolean localStateRecorded;

    /**
     * Last stable state; indicates the last recorded state
     */
    private long[] lastRecordedState;

    /**
     * Will hold a set of names of the processes which are expected to produce a marker
     */
    private Set<String> awaitingMarker;

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
        this.awaitingMarker = new HashSet<>();

        this.separatingQueue = new ArrayList<>();

        /*
         * Assume that in the first half one holds the last received time message
         * from that process, and in the latter half, the last sent message
         */
        this.state = new long[2 * numberOfProcesses];
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
        /* Add the message to the buffer queue */
        separatingQueue.add(message);
    }

    /**
     * Process messages which have been stored in the process' input buffer
     */
    public void processIncomingMessages() throws RemoteException {

        for (Message message : separatingQueue) {

            /* Get the source's PID to increase efficiency */
            long sourcePid = message.getPid();

            /* We should check that indeed the new value will be maximal. It should never happen, but just to make sure */
            state[(int) sourcePid * 2] = Math.max(message.getsClock(), state[(int) sourcePid * 2]);

            /* If the system is currently recording its state */
            if (localStateRecorded) {
                if (message.getType() == MessageType.MARKER) {
                    awaitingMarker.remove(message.getProcName());

                    /* If no more processes require ack */
                    if (awaitingMarker.isEmpty()) {
                        System.out.println("Finished recording my own state: " + Arrays.toString(lastRecordedState));
                        System.out.println("Following are the link states:");

                        /* Print the states of the incoming links */
                        for (Iterator<Map.Entry<String, Queue<Message>>> entryIterator = incomingLinks.entrySet().iterator(); entryIterator.hasNext(); ) {
                            Map.Entry<String, Queue<Message>> pair = entryIterator.next();

                            Queue<Message> queue = pair.getValue();

                            System.out.println("(" + pid + ") Incoming link from " + pair.getKey());

                            for (Message queueMessage : queue)
                                System.out.println("(" + pid + ") : " + queueMessage.getsClock());

                            queue.clear();
                            entryIterator.remove();
                        }
                    }
                } else
                    /* Add the message to its corresponding queue */
                    incomingLinks.get(message.getProcName()).add(message);
            } else if (message.getType() == MessageType.MARKER) {
                /* If marker message, then prepare for state; else drop regular messages since they are irrelevant  */
                localStateRecorded = true;

                /* Store the current state, as the recorded state */
                lastRecordedState = Arrays.copyOf(state, state.length);

                /* Configure the names of the processes from which markers will be required. Remove source, and this node */
                awaitingMarker.addAll(incomingLinks.keySet());
                awaitingMarker.remove(message.getProcName());
                awaitingMarker.remove(name);

                /* Make sure all queues are clear before proceeding */
                for (Queue<Message> messages : incomingLinks.values())
                    messages.clear();

                /* Send marker to the other processes, requesting they record their state */
                sendMessageToEveryoneWithExceptions(
                        new Message(
                                pid,
                                name,
                                sClock++,
                                MessageType.MARKER,
                                "Requesting state"
                        ),
                        new HashSet<String>(Arrays.asList(message.getProcName(), name))
                );
            }
        }

    }

    /**
     * Send a message to all other peers (including itself)
     *
     * @param message the message being sent
     * @param exception a set of String identifying processes to which one shouldn't send the message
     * @throws RemoteException
     */
    private void sendMessageToEveryoneWithExceptions(Message message, Set exception) throws RemoteException {

        for (Map.Entry<String, CommunicationChannel> stringCommunicationChannelEntry : outgoingLinks.entrySet())
            if (!exception.contains(stringCommunicationChannelEntry.getKey()))
                stringCommunicationChannelEntry.getValue().sendMessage(message);

    }

    /**
     * Deliver all messages that have been received.
     */
    public void processReceivedMessages() {

        for (Map.Entry<String, Queue<Message>> stringQueueEntry : incomingLinks.entrySet()) {
            Queue<Message> messages = stringQueueEntry.getValue();

            while (!messages.isEmpty()) {
                // Deliver each message
                Message message = messages.poll();

                // TODO: Do something with message

                if(message.getType().equals(MessageType.MARKER)){
                    if(!localStateRecorded){
                        // TODO: record state of c as empty and start procedure record_and_send_markers
                    } else {
                        // TODO: record state of c as contents of B_c
                    }
                }
            }
        }



    }

    private void sendMarker() throws RemoteException {

        Message markerMessage = new Message(pid, name, sClock++, MessageType.MARKER, "This is a marker message");

        lastRecordedState = Arrays.copyOf(state, state.length);
        localStateRecorded = true;

        for (Map.Entry<String, CommunicationChannel> stringCommunicationChannelEntry : outgoingLinks.entrySet())
            if (!stringCommunicationChannelEntry.getKey().equals(name))
                stringCommunicationChannelEntry.getValue().sendMessage(markerMessage);

    }

    @Override
    public void run() {



        /* Find peers, and establish incoming, and outgoing connections to them */
        try {
            findPeers();
        } catch (Exception e) {
            System.err.println("Encountered some error: " + e.getMessage());

            e.printStackTrace();

            System.exit(0xFF);
        }

        if (pid == 0)
            try {
                sendMarker();
            } catch (RemoteException e) {
                e.printStackTrace();
            }

        /* The infinite loop */
        while(true) {

            try {
                processIncomingMessages();
            } catch (RemoteException e) {
                System.err.println("Encountered an RMI error: " + e.getMessage());

                e.printStackTrace();
            }
            // processReceivedMessages();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("Thread " + name + "Was interrupted whilst waiting");

                e.printStackTrace();
            }
        }

    }
}
