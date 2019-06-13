package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class Data  implements Serializable {
    public String key;
    public String value;
    public Code code;
    public String DestinationPort;
    public String SourcePort;
    public Data(String k, String v, Code c, String d, String s)
    {
        key=k;
        value=v;
        code=c;
        DestinationPort =d;
        SourcePort = s;
    }

}
