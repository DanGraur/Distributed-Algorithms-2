package node;

import node.message.Message;
import node.message.MessageType;
import node.remote.CommunicationChannel;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
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
    private final Queue<Message> separatingQueue;

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

    private PrintWriter log;

    /**
     * Will hold the communication channels to the other peers
     */
    private Map<String, CommunicationChannel> peers;

    /**
     * Class Constructor
     *
     * @param registry a reference to the RMI registry
     * @param numberOfProcesses the total number of nodes in the system
     */
    public Component(String name, long pid, Registry registry, int numberOfProcesses) {
        try{
            String path = new File("").getAbsolutePath();
            if(path.contains("target\\classes")) path = path.replace("target\\classes", "");
            log = new PrintWriter(path + "/logs/log_component_" + pid + ".txt", "UTF-8");
            log.println(new Date().toString() + " Initialized Component with process id " + pid);
            log.flush();
        } catch (IOException e){
            e.printStackTrace();
        }

        this.registry = registry;

        this.name = name;
        this.pid = pid;
        this.incomingLinks = new HashMap<>();
        this.outgoingLinks = new HashMap<>();
        this.awaitingMarker = new HashSet<>();

        this.separatingQueue = new LinkedBlockingQueue<>();

        /*
         * Assume that in the first half one holds the last received time message
         * from that process, and in the latter half, the last sent message
         */
        this.state = new long[2 * numberOfProcesses];
    }

    public void setPeers(Map<String, CommunicationChannel> peers) {
        this.peers = peers;
        log.print(new Date().toString() + " Set new peers with process ids: ");
        for(String peerID : peers.keySet()){
            log.print(peerID + " ");
        }
        log.println();
        log.flush();
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

        log.println(new Date().toString() + " LINKS: My Incoming Links:");
        incomingLinks.forEach((key, value) -> log.println(new Date().toString() + String.format("(%d) Incoming: %s", pid, key)));
        System.out.println(String.format("(%d) Printing my incoming links;", pid));
        incomingLinks.forEach((key, value) -> System.out.println(String.format("(%d) Incoming: %s", pid, key)));

        log.println(new Date().toString() + " LINKS: My outgoing Links:");
        incomingLinks.forEach((key, value) -> log.println(new Date().toString() + String.format("(%d) Outgoing: %s", pid, key)));
        System.out.println(String.format("(%d) Printing my outgoing links;", pid));
        outgoingLinks.forEach((key, value) -> System.out.println(String.format("(%d) Outgoing: %s", pid, key)));
        log.flush();
        
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
     * Send a message to all other peers (including itself)
     *
     * @param message the message being sent
     * @throws RemoteException
     */
    private void sendMessageToOutgoingLinks(Message message) throws RemoteException {
        for(Iterator<Map.Entry<String, CommunicationChannel>> entryIterator = outgoingLinks.entrySet().iterator();
            entryIterator.hasNext();){
            Map.Entry<String, CommunicationChannel> pair = entryIterator.next();

            pair.getValue().sendMessage(message);
        }
    }

    /**
     * Process messages which have been stored in the process' input buffer
     */
    private void processIncomingMessages() throws RemoteException {

        while (!separatingQueue.isEmpty()) {
            Message message = separatingQueue.poll();

            log.println(new Date().toString() + " Received message: " + message.toString());
            System.out.println("(" + pid + ") Received a message: " + message.toString());

            /* Get the source's PID to increase efficiency */
            long sourcePid = message.getPid();

            /* We should check that indeed the new value will be maximal. It should never happen, but just to make sure */
            state[(int) sourcePid * 2] = Math.max(message.getsClock(), state[(int) sourcePid * 2]);

            /* If the system is currently recording its state */
            if (localStateRecorded) {
                log.println(new Date().toString() + " Local state was recorded and now processing message ");
                if (message.getType() == MessageType.MARKER) {
                    log.println(new Date().toString() + " Marker received from component " +  message.getPid() +
                            " and can be removed from awaited markers");
                    awaitingMarker.remove(message.getProcName());

                    /* If no more processes require ack */
                    if (awaitingMarker.isEmpty()) {
                        log.println(new Date().toString() + " Received all expected markers, " +
                                "finished recording local recorded state: " + Arrays.toString(lastRecordedState));
                        System.out.println("(" + pid + ") Finished recording my own state: " + Arrays.toString(lastRecordedState));
                        System.out.println("(" + pid + ") Following are the link states:");

                        /* Print the states of the incoming links */
                        log.println(new Date().toString() + " Now printing all states of the incoming links: ");

                        if(incomingLinks.isEmpty()){
                            System.out.println(" EMPTY -----------------------");

                        }

                        for (Iterator<Map.Entry<String, Queue<Message>>> entryIterator = incomingLinks.entrySet().iterator();
                             entryIterator.hasNext(); ) {
                            //System.out.println("--------------------------------------------------------------");
                            Map.Entry<String, Queue<Message>> pair = entryIterator.next();

                            Queue<Message> queue = pair.getValue();

                            System.out.println("(" + pid + ") Incoming link from " + pair.getKey());
                            log.println(new Date().toString() + " Incoming link from: " + pair.getKey());

                            if (queue.isEmpty()) {
                                log.println(new Date().toString() + " Message queue of link is empty ");
                            } else {
                                for (Message queueMessage : queue) {
                                    log.println(new Date().toString() + " Message clock in link: " + queueMessage.getsClock());
                                    System.out.println("(" + pid + ") : " + queueMessage.getsClock());
                                }
                            }
                            queue.clear();
//                            System.out.println(incomingLinks.size());
//                            entryIterator.remove();
//                            System.out.println(incomingLinks.size());

                        }

                        /* The process is no longer recording its state */
                        log.println(new Date().toString() + " Setting localStateRecorded back to false");
                        localStateRecorded = false;
                    }
                } else if(message.getType() == MessageType.REGULAR){
                    log.println(new Date().toString() + " Received REGULAR message with content: " + message.getContents());

                    /* Add the message to its corresponding queue */
                    if(awaitingMarker.contains(message.getProcName())){
                        System.out.println("-----------------------------------------");
                        incomingLinks.get(message.getProcName()).add(message);
                    }

                }
            } else if (message.getType() == MessageType.MARKER) {
                log.println(new Date().toString() + " Local state was not yet recorded and will now occur ");
                /* If marker message, then prepare for state; else drop regular messages since they are irrelevant  */
                localStateRecorded = true;

                /* Store the current state, as the recorded state */
                lastRecordedState = Arrays.copyOf(state, state.length);
                log.println(new Date().toString() + " Local state recorded is now: " + Arrays.toString(lastRecordedState));

                /* Configure the names of the processes from which markers will be required. Remove source, and this node */
                awaitingMarker.addAll(incomingLinks.keySet());
                awaitingMarker.remove(message.getProcName());
                awaitingMarker.remove(name);

                /* Make sure all queues are clear before proceeding */
                for (Queue<Message> messages : incomingLinks.values())
                    messages.clear();

                /* Send marker to the other processes, requesting they record their state */
                log.println(new Date().toString() + " Now sending marker to other processes to request their state ");
                sendMessageToEveryoneWithExceptions(
                        new Message(
                                pid,
                                name,
                                ++sClock,
                                MessageType.MARKER,
                                "Requesting state"
                        ),
                        new HashSet<String>(Arrays.asList(name))
                );
            }
            log.flush();
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

    private void sendMarker() throws RemoteException {

        Message markerMessage = new Message(pid, name, ++sClock, MessageType.MARKER, "This is a marker message");

        lastRecordedState = Arrays.copyOf(state, state.length);
        localStateRecorded = true;

        for (Map.Entry<String, CommunicationChannel> stringCommunicationChannelEntry : outgoingLinks.entrySet())
            if (!stringCommunicationChannelEntry.getKey().equals(name))
                stringCommunicationChannelEntry.getValue().sendMessage(markerMessage);

    }

    @Override
    public void run() {

        System.out.println("I'm in the run method " + name);


        /* Find peers, and establish incoming, and outgoing connections to them */
        try {
            findPeers();
        } catch (Exception e) {
            System.err.println("Encountered some error: " + e.getMessage());

            e.printStackTrace();

            System.exit(0xFF);
        }

        // Randomly send a message as process 0
        if(pid == 0){
            try{
                log.println(new Date().toString() + " This component has send the first REGULAR message. ");
                log.flush();
                Message message = new Message(pid, name, sClock++, MessageType.REGULAR, "First message from component" + pid);
                sendMessageToOutgoingLinks(message);
            } catch (RemoteException e){
                e.printStackTrace();
            }
        }

        // Randomly try to record the global state as process 2
        if (pid == 2)
            try {
                sendMarker();

                log.println(new Date().toString() + " This component has send the first MARKER. ");
                log.flush();
                System.out.println("Have sent marker: " + pid);
            } catch (RemoteException e) {
                e.printStackTrace();
            }

        // Randomly send a message as process 1
        if(pid == 1){
            try {
                processIncomingMessages();
            } catch (RemoteException e) {
                System.err.println("Encountered an RMI error: " + e.getMessage());
                e.printStackTrace();
            }

            // Delay before sending a message
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try{
                log.println(new Date().toString() + " This component has send the second REGULAR message. ");
                log.flush();
                Message message = new Message(pid, name, sClock++, MessageType.REGULAR, "Second message from component " + pid);
                sendMessageToOutgoingLinks(message);
            } catch (RemoteException e){
                e.printStackTrace();
            }
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
