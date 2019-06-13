package edu.buffalo.cse.cse486586.groupmessenger2;

public class HoldBackMessage {
    public String message;
    public String port;
    public int tempseq;

    boolean deliverable;

    public HoldBackMessage(String msg, String prt, int proposed_seq, boolean delvry)
    {
        message=msg;
        port=prt;
        tempseq=proposed_seq;
        deliverable=delvry;
    }

}
