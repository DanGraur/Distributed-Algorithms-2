package node.message;

/**
 * @author Dan Graur 11/25/2017
 */
public enum MessageType {
    REGULAR,
    ACK,
    RELEASE,
    /**
     * Message sent when state recording is performed
     */
    REC_STATE
}
