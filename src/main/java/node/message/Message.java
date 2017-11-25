package node.message;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author Dan Graur 11/25/2017
 */
public class Message implements Comparable {

    private String messageId;
    private String procName;
    private long pid;
    private long sClock;
    private MessageType type;
    private String contents;
    private Date timestamp;
    private Set<String> acks;
    private boolean canRelease;

    public Message(long pid, String procName, long sClock, MessageType type, String contents) {
        messageId = UUID.randomUUID().toString();

        this.procName = procName;
        this.pid = pid;
        this.sClock = sClock;
        this.type = type;
        this.contents = contents;
        this.timestamp = new Date();
        this.acks = new HashSet<>();
        this.canRelease = false;
    }

    public String getProcName() {
        return procName;
    }

    public void setProcName(String procName) {
        this.procName = procName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setContents(String contents) {
        this.contents = contents;
    }

    public long getPid() {
        return pid;
    }

    public long getsClock() {
        return sClock;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getContents() {
        return contents;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void addAck(String peer) {
        acks.add(peer);
    }

    public boolean containsAll(Set peers) {
        return acks.containsAll(peers);
    }

    public boolean isCanRelease() {
        return canRelease;
    }

    public void setCanRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public int compareTo(Object o) {

        if (!(o instanceof Message))
            return -1;

        Message that = (Message) o;

        int diff = (int) (this.sClock - that.getsClock());

        /* If equal timestamps --> use pid's to break contention */
        if (diff == 0)
            return (int) (this.pid - that.getPid());

        /* Else use only the sClocks */
        return diff;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", procName='" + procName + '\'' +
                ", pid=" + pid +
                ", sClock=" + sClock +
                ", type=" + type +
                ", contents='" + contents + '\'' +
                ", timestamp=" + timestamp +
                ", acks=" + acks +
                ", canRelease=" + canRelease +
                '}';
    }

}
