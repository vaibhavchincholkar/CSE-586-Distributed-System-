package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDhtProvider extends ContentProvider {
    private PA3DbHelper pa3DbHelper;
    String myPort,myID,preNode,succNode,preID,succID;
    String msg="GIVEMEPREANDSUCC";
    static final String ACK_MSG="ACK";
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    String[] ar ={REMOTE_PORT0,REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    List<String> nodeIDs = Collections.synchronizedList(new ArrayList<String>());
    List<String> liveports = Collections.synchronizedList(new ArrayList<String>());
    HashMap<String, String>  portandid = new HashMap<String, String>();
    HashMap<String, String>  storage = new HashMap<String, String>();
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.d("My log","selection parameter is"+selection);
        if(selection.equals("*"))
        {
            SQLiteDatabase db = pa3DbHelper.getWritableDatabase();
            db.delete(dbcontract.TABLE_NAME,null,null);
            db.close();
            if(!succID.equals(myID))//check if its the only node in the ring
            {
                String msgTosend="!";
                new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgTosend, succNode);
            }
        }
        else if(selection.equals("@"))
        {
            SQLiteDatabase db = pa3DbHelper.getWritableDatabase();
            db.delete(dbcontract.TABLE_NAME,null,null);
            db.close();
        }
        else
        {
            SQLiteDatabase db = pa3DbHelper.getWritableDatabase();
            db.delete(dbcontract.TABLE_NAME,dbcontract.COLUMN_KEY + " = ?" ,new String[]{selection});
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        String keyhash = null;
        String receivedKey=values.getAsString(dbcontract.COLUMN_KEY);
        keyhash=genHash(receivedKey);
        if(check(keyhash))
        {
            //in current node's authority
            long insrt=-1;
            ContentValues change = new ContentValues();
            change.put(dbcontract.COLUMN_KEY,values.getAsString(dbcontract.COLUMN_KEY));
            change.put(dbcontract.COLUMN_VALUE,values.getAsString(dbcontract.COLUMN_VALUE));
            final SQLiteDatabase db = pa3DbHelper.getWritableDatabase();
            db.beginTransaction();
            insrt=db.insert(dbcontract.TABLE_NAME,null,change);
            if(insrt==-1)
            {
                db.endTransaction();
                Log.d("pa2insert","failed to insert");
                return null;
            }
            else
            {
                db.setTransactionSuccessful();
                db.endTransaction();
                Log.d("pa2insert","value:"+values.getAsString("value")+ " key:"+values.getAsString("key"));
                return uri;
            }
        }
        else
        {
            String msgToInsert=values.getAsString(dbcontract.COLUMN_KEY)+"%"+values.getAsString(dbcontract.COLUMN_VALUE);
            Log.d("My log","NOT in my authority sending it to successor"+succNode+" msg = "+msgToInsert);
            new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToInsert, succNode);
        }
        return uri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort  = String.valueOf((Integer.parseInt(portStr) * 2));
        succNode=myPort;
        preNode=myPort;
        try
        {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if(myPort.equals("11108")) {
                liveports.add(myPort);//add live port

            } else {
                new ClientInitialization().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
            myID = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
            nodeIDs.add(myID);
            portandid.put(myID,myPort);
            preID=myID;
            succID=myID;
        }
        catch (Exception e)
        {
            Log.e("socket error",""+e);
        }

        Log.d("mypa3id","="+myID+" My port is="+myPort);

        pa3DbHelper= new PA3DbHelper(getContext());
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Cursor cursor=null;
        Log.d("My log", "Inside query wit selection="+selection);
        if(selection.equals("*"))
        {
            Log.d("My log", "Inside * if block");
            storage.clear();
            String msgTosend="geteverything"+"#"+myPort;
            new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgTosend, succNode);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            MatrixCursor cursor2 = new MatrixCursor(new String[]{dbcontract.COLUMN_KEY, dbcontract.COLUMN_VALUE});
            SQLiteDatabase db = pa3DbHelper.getReadableDatabase();
            try {
                cursor = db.query(dbcontract.TABLE_NAME, null,null ,null , null, null, null, null);
            } catch (Exception e) {
                Log.d("The error is in query", "=" + e);
            }
            Cursor temp=cursor;
            while (cursor.moveToNext()) {
                cursor2.addRow(new String[]{cursor.getString(cursor.getColumnIndex(dbcontract.COLUMN_KEY)),cursor.getString(cursor.getColumnIndex(dbcontract.COLUMN_VALUE))});
            }
            Log.d("My log", "storage size" + storage.size());
            for (Map.Entry mapElement : storage.entrySet()) {
                String key = (String) mapElement.getKey();
                String value = (String) mapElement.getValue();
                cursor2.addRow(new String[]{key, value});
            }
            return cursor2;
        }
        if(selection.equals("@"))
        {
            Log.d("My log", "Inside @ if block");
            SQLiteDatabase db = pa3DbHelper.getReadableDatabase();
            try {
                cursor = db.query(dbcontract.TABLE_NAME, null,null ,null , null, null, null, null);
            } catch (Exception e) {
                Log.d("The error is in query", "=" + e);
            }
        }
        else
        {
            if(check(genHash(selection)))
            {
                SQLiteDatabase db = pa3DbHelper.getReadableDatabase();
                try {
                    cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?" ,new String[]{selection} , null, null, null, null);
                } catch (Exception e) {
                    Log.d("The error is in query", "=" + e);
                }
                return cursor;
            }
            else
            {
                String msgTosend=selection+"-"+myPort;
                storage.clear();
                Log.d("My log", "sending message to succ");
                new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgTosend, succNode);
                try {
                    if(uri!=null)
                    {
                        Thread.sleep(1000);
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d("My log", "after waiting storage size="+storage.size());
                MatrixCursor cursor2 = new MatrixCursor(new String[]{dbcontract.COLUMN_KEY, dbcontract.COLUMN_VALUE});
                for (Map.Entry mapElement : storage.entrySet()) {
                    String key = (String) mapElement.getKey();
                    String value = (String) mapElement.getValue();
                    cursor2.addRow(new String[]{key, value});
                }
                return cursor2;
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private boolean check(String keyhash) {

        if(preID.equalsIgnoreCase(myID)) {
            return true;
        }
        else if(myID.compareTo(preID)>0)
        {
            Log.d("My log","Pre node less than my node keyhash compare = "+keyhash.compareTo(myID));
            if(keyhash.compareTo(myID)<=0&&keyhash.compareTo(preID)>0)
            {
                return  true;
            }
            else
            {
                return false;
            }
        }
        else if(myID.compareTo(preID)<0)//cyclic case
        {
            Log.d("My log","Pre node greater than my node keyhash compare = "+keyhash.compareTo(myID));
            if(keyhash.compareTo(preID)>0||keyhash.compareTo(myID)<=0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        return false;
    }

    private String genHash(String input) {
        try{
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        }
        catch (Exception e)
        {
            Log.d("Exception","in hash function"+e);
        }
        return null;
    }

    //initializing client with predesessor and successor node
    private class ClientInitialization extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            String msgToSend = msgs[0];
            try
            {
                msgToSend=msgToSend.substring(0,msgToSend.length()-1)+"*"+myPort+"\n";
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                if(msgToSend.isEmpty()||msgToSend.equals("null")||msgToSend.equals(""))
                {
                    //do nothing
                }
                else
                {
                    out.print(msgToSend);
                    out.flush();
                }
                String temp;
                try
                {
                    temp=input.readLine();
                    Log.d("My log", "received string is"+temp);
                    if(temp.contains("*"))
                    {
                        int index=temp.indexOf('*');
                        preNode=temp.substring(0,index);
                        succNode=temp.substring(index+1);
                        preID=genHash(String.valueOf(Integer.parseInt(preNode) / 2));
                        Log.d("My log", "pre node = "+preNode +" pre id ="+preID);
                        socket.close();
                    }
                }
                catch (Exception e) {
                    Log.d("Exception", "Exception at client side :="+e);
                }

            }
            catch (UnknownHostException e) {
                Log.e("error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("error", "ClientTask socket IOException");
            }
            return null;
        }
    }
    //query successor with the selection parameter
    private class SendMessageToSuccessor extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket=null;
            String msgToSend = msgs[0];
            msgToSend=msgToSend+"\n";
            try
            {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succNode));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                out.print(msgToSend);
                out.flush();
            }
            catch (UnknownHostException e)
            {
                Log.e("error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("error", "ClientTask socket IOException");
            }
            return null;
        }
    }

    //server task
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader input = null;
            PrintWriter out=null;
            Socket soc=null;
            String servermsg = "";

            while(!isCancelled())
            {
                try
                {
                    soc = serverSocket.accept();//Listens for a connection to be made to this socket and accepts it. The method blocks until a connection is made.
                    input = new BufferedReader(new InputStreamReader(soc.getInputStream()));
                    servermsg = input.readLine();
                    if(servermsg.equals("")||servermsg.isEmpty())
                    {
                        //dont do anything
                    }
                    else if(servermsg.contains("*"))//request to 5546 to get its predecessor and successor
                    {
                        int index=servermsg.indexOf('*');
                        String new_msg=servermsg.substring(0,index);//its just text which is not important
                        String port=servermsg.substring(index+1);
                        Log.d("My log","port value="+port);

                        String nodeid=genHash(String.valueOf(Integer.parseInt(port)/2));
                        nodeIDs.add(nodeid);
                        Collections.sort(nodeIDs);

                        String response="";
                        int location=0;
                        if(liveports.size()==1)//adding first node into the ring except 5546
                        {
                            response=myPort+"+"+myPort;
                            preNode=port;
                            preID=genHash(String.valueOf(Integer.parseInt(preNode) / 2));
                            succNode=port;
                            succID=genHash(String.valueOf(Integer.parseInt(succNode) / 2));
                        }
                        else //adding more nodes into the ring
                        {
                            try
                            {
                                for(location=0;location<nodeIDs.size();location++) {
                                    if (nodeIDs.get(location).equalsIgnoreCase(nodeid)) {
                                        break;
                                    }
                                }
                                if(location==nodeIDs.size()-1)
                                {
                                    String predecessor=portandid.get(nodeIDs.get(location-1));
                                    String successor=portandid.get(nodeIDs.get(0));
                                    response=predecessor+"+"+successor;
                                }
                                else if(location==0)
                                {
                                    String predecessor=portandid.get(nodeIDs.get(liveports.size()-1));
                                    String successor=portandid.get(nodeIDs.get(1));
                                    response=predecessor+"+"+successor;
                                }
                                else
                                {
                                    String predecessor=portandid.get(nodeIDs.get(location-1));
                                    String successor=portandid.get(nodeIDs.get(location+1));
                                    response=predecessor+"+"+successor;
                                }
                            }
                            catch (Exception e)
                            {
                                Log.d("Exception in joining ",""+e);
                            }
                        }

                        portandid.put(nodeid,port);
                        Collections.sort(nodeIDs);
                        liveports.add(port);
                        Collections.sort(liveports);

                        out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(soc.getOutputStream())),true);
                        out.print(response);
                        out.flush();
                        //now reset all avds successor and predecessor based on new added node
                        Socket sockettemp;
                        String temppreid="",tempsuccid="";
                        for(int i=0;i<liveports.size();i++)
                        {
                            String tempid=genHash(String.valueOf(Integer.parseInt(liveports.get(i))/2));
                            sockettemp = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(liveports.get(i)));
                            PrintWriter outtemp = new PrintWriter(new BufferedWriter(new OutputStreamWriter(sockettemp.getOutputStream())), true);
                            for(location=0;location<nodeIDs.size();location++) {
                                if (nodeIDs.get(location).equalsIgnoreCase(tempid)) {
                                    break;
                                }
                            }
                            if(location==0)
                            {
                                temppreid=nodeIDs.get(nodeIDs.size()-1);
                                tempsuccid=nodeIDs.get(1);
                            }
                            else if(location==nodeIDs.size()-1)
                            {
                                temppreid=nodeIDs.get(location-1);
                                tempsuccid=nodeIDs.get(0);
                            }
                            else
                            {
                                temppreid=nodeIDs.get(location-1);
                                tempsuccid=nodeIDs.get(location+1);
                            }
                            String temppreNode=portandid.get(temppreid);
                            String tempsuccNode=portandid.get(tempsuccid);
                            String msgtemp=temppreNode+"+"+tempsuccNode+"\n";
                            Log.d("My log","pre = "+preNode +" succ = "+succNode);
                            outtemp.print(msgtemp);
                            outtemp.flush();
                        }

                    }
                    else if(servermsg.contains("+"))//respomse from 5546 regarding pre and succ of that node
                    {
                        int index=servermsg.indexOf('+');
                        String prePort=servermsg.substring(0,index);
                        String succPort=servermsg.substring(index+1);

                        succNode=succPort;
                        succID=genHash(succNode);
                        preNode=prePort;
                        preID=genHash(String.valueOf(Integer.parseInt(preNode) / 2));
                    }
                    else if(servermsg.contains("%"))//insertion message from predecessor
                    {
                        Log.d("My log","received message from the successor");
                        try
                        {
                            int index=servermsg.indexOf('%');
                            String receivedKey=servermsg.substring(0,index);
                            String reeivedValue=servermsg.substring(index+1);
                            Log.d("My log","Message received from other node key="+receivedKey);
                            ContentValues change = new ContentValues();
                            change.put(dbcontract.COLUMN_KEY,receivedKey);
                            change.put(dbcontract.COLUMN_VALUE,reeivedValue);

                            insert(null,change);
                        }
                        catch (Exception e)
                        {
                            Log.e("My log","Error in % message received");
                        }

                    }
                    else if(servermsg.contains("-"))//query from predecessor for particular key
                    {
                        Log.d("My log","inside msg -");
                        try
                        {
                            int index=servermsg.indexOf('-');
                            String selectionkey=servermsg.substring(0,index);
                            String senderport=servermsg.substring(index+1);
                            if(!senderport.equalsIgnoreCase(myPort))
                            {
                                if(check(genHash(selectionkey)))
                                {
                                    Log.d("My log","I have the qury value");
                                    Cursor sendercursor=query(null,null,selectionkey,null,null);
                                    while (sendercursor.moveToNext()) {
                                        String msgTOsend=sendercursor.getString(sendercursor.getColumnIndex(dbcontract.COLUMN_KEY))+"$"+sendercursor.getString(sendercursor.getColumnIndex(dbcontract.COLUMN_VALUE));
                                        Socket sendersoc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(senderport));
                                        PrintWriter succout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(sendersoc.getOutputStream())), true);
                                        succout.print(msgTOsend+"\n");
                                        succout.flush();
                                    }
                                }
                                else
                                {
                                    Log.d("My log","I dnt have the qury value");
                                    Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succNode));
                                    PrintWriter succout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(succsocket.getOutputStream())), true);
                                    succout.print(servermsg+"\n");
                                    succout.flush();
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Log.d("My log","Exception in server task if -");
                        }
                    }
                    else if(servermsg.contains("$"))//msg from some node in the ring which has the value and then store the value in the hash map
                    {
                        Log.d("My log","inside msg $");
                        int index=servermsg.indexOf('$');
                        String reckey=servermsg.substring(0,index);
                        String recvalue=servermsg.substring(index+1);
                        storage.put(reckey,recvalue);

                    }
                    else if(servermsg.contains("#"))//its request from predecessor to send all the values of the database to a node
                    {
                        int index=servermsg.indexOf('#');
                        String tp=servermsg.substring(0,index);
                        String senderport=servermsg.substring(index+1);
                        if(!senderport.equalsIgnoreCase(myPort))//check if ring is completed or not
                        {
                            Cursor sendercursor=query(null,null,"@",null,null);
                            while (sendercursor.moveToNext()) {
                                String msgTOsend=sendercursor.getString(sendercursor.getColumnIndex(dbcontract.COLUMN_KEY))+"$"+sendercursor.getString(sendercursor.getColumnIndex(dbcontract.COLUMN_VALUE));
                                Socket sendersoc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(senderport));
                                PrintWriter succout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(sendersoc.getOutputStream())), true);
                                succout.print(msgTOsend+"\n");
                                succout.flush();
                            }

                            Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succNode));
                            PrintWriter succout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(succsocket.getOutputStream())), true);
                            succout.print(servermsg+"\n");
                            succout.flush();
                        }
                    }
                    else if(servermsg.contains("!"))//msg from predecessor to delete the the entire table
                    {
                        delete(null,"*",null);
                        Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succNode));
                        PrintWriter succout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(succsocket.getOutputStream())), true);
                        succout.print(servermsg+"\n");
                        succout.flush();
                    }
                }
                catch (Exception e)
                {
                    Log.d("Exception1 ",""+e);
                }
                finally
                {
                    if (soc != null)
                    {
                        try
                        {
                            soc.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

            return;
        }
    }

}
