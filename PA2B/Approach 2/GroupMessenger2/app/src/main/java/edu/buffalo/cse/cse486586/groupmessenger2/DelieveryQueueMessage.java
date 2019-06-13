package edu.buffalo.cse.cse486586.groupmessenger2;

public class DelieveryQueueMessage {
    public String DMport;
    public int DMagr_seq;
    public String DMMsgToInsert;

    public DelieveryQueueMessage(int seq, String prt, String MsgToInsrt)
    {
        DMport=prt;
        DMagr_seq=seq;
        DMMsgToInsert=MsgToInsrt;
    }
}
