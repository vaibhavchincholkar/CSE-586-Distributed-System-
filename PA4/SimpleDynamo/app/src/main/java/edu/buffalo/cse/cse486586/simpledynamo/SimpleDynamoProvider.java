package edu.buffalo.cse.cse486586.simpledynamo;

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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

enum Code
{
    REPLICATION, COORDINATOR, GETALLMESSAGE, GETSINGLEMESSAGE, RETURNALLMSGS, RETURNSINGLEMSG, DELETE, RECOVERYMSG, GETRECOVERY, RECOVERYFROMTHESUCC, ACK, REPLICATIONRECOVERY;
}
public class SimpleDynamoProvider extends ContentProvider {
	private PA4DbHelper pa4DbHelper;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	static final String TAG ="PA4LOG";
    String[] ar ={REMOTE_PORT0,REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    HashMap<String, String>  allPortsIDs = new HashMap<String, String>();
    String successor1,successor2;
    String myPort,myID,preID,preID2,preID3;
    List<String> nodeIDs = Collections.synchronizedList(new ArrayList<String>());
    HashMap<String, String>  storage = new HashMap<String, String>();
    HashMap<String, String>  recovery = new HashMap<String, String>();
    static final String ACK="ACK";
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("*"))
        {
            SQLiteDatabase db = pa4DbHelper.getWritableDatabase();
            db.delete(dbcontract.TABLE_NAME,null,null);
            db.close();
            for(String port : ar)
            {
                Data deleteAllMsg = new Data("","",Code.DELETE,port,myPort);// we do not send any key or value
                new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, deleteAllMsg);
            }
        }
        else if(selection.equals("@"))
        {
            SQLiteDatabase db = pa4DbHelper.getWritableDatabase();
            db.delete(dbcontract.TABLE_NAME,null,null);
            db.close();
        }
        else
        {
            SQLiteDatabase db = pa4DbHelper.getWritableDatabase();
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
	public Uri insert(Uri uri, ContentValues values) {
        String receivedKey=values.getAsString(dbcontract.COLUMN_KEY);
        String keyhash=genHash(receivedKey);
        if(uri==null)//which means its a call from server task and store the data (either replication or authority)
        {
            Log.d(TAG,"replica insertion key:"+values.getAsString(dbcontract.COLUMN_KEY));
            long insrt=-1;
            ContentValues change = new ContentValues();
            change.put(dbcontract.COLUMN_KEY,values.getAsString(dbcontract.COLUMN_KEY));
            change.put(dbcontract.COLUMN_VALUE,values.getAsString(dbcontract.COLUMN_VALUE));
            final SQLiteDatabase db = pa4DbHelper.getWritableDatabase();
            db.beginTransaction();
            try
            {
                insrt=db.insert(dbcontract.TABLE_NAME,null,change);
            }
            catch (Exception e)
            {
                Log.d(TAG,"failed to insert because"+e);
            }

            if(insrt==-1)
            {
                db.endTransaction();
                Log.d(TAG,"failed to insert");
            }
            else
            {
                db.setTransactionSuccessful();
                db.endTransaction();
                Log.d(TAG,"Inserting replica value:"+values.getAsString("value")+ " key:"+values.getAsString("key"));
            }
            return uri;
        }
        else
        {
            if(check(keyhash))
            {
                Log.d(TAG,"In my authority Inserting key: "+ values.get(dbcontract.COLUMN_KEY));
                //in current node's authority
                long insrt=-1;
                ContentValues change = new ContentValues();
                change.put(dbcontract.COLUMN_KEY,values.getAsString(dbcontract.COLUMN_KEY));
                change.put(dbcontract.COLUMN_VALUE,values.getAsString(dbcontract.COLUMN_VALUE));
                final SQLiteDatabase db = pa4DbHelper.getWritableDatabase();
                db.beginTransaction();
                insrt=db.insert(dbcontract.TABLE_NAME,null,change);
                if(insrt==-1)
                {
                    db.endTransaction();
                    Log.d(TAG,"failed to insert");
                    return null;
                }
                else
                {
                    db.setTransactionSuccessful();
                    db.endTransaction();
                    Log.d(TAG,"Inserting my value:"+values.getAsString("value")+ " key:"+values.getAsString("key"));
                    //now we need to replicate this key to our two successor
                    Log.d(TAG,"Replicating the inserted value");
                    Data rep1 = new Data(change.getAsString(dbcontract.COLUMN_KEY),change.getAsString(dbcontract.COLUMN_VALUE),Code.REPLICATION,successor1,myPort);
                    Data rep2 = new Data(change.getAsString(dbcontract.COLUMN_KEY),change.getAsString(dbcontract.COLUMN_VALUE),Code.REPLICATION,successor2,myPort);

                    new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,rep1);
                    new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, rep2);
                }
                return uri;
            }
            else
            {
                //now we need to find the coordinator of the key
                String coordinatorPort=findCoordinatorPort(keyhash); //get the coordinator port
                Log.d(TAG,"Not in my authority key:"+ values.get(dbcontract.COLUMN_KEY)+" sending it to port:"+coordinatorPort+" my port:"+myPort);

                Data msgforco = new Data(values.getAsString(dbcontract.COLUMN_KEY),values.getAsString(dbcontract.COLUMN_VALUE),Code.COORDINATOR,coordinatorPort,myPort);
                new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgforco);//send the message to the coordinator
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return uri;
            }
        }
	}

	@Override
	public boolean onCreate() {
	    recovery.clear();
        pa4DbHelper= new PA4DbHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort  = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG,"my port:"+myPort);
        myID = genHash(String.valueOf(Integer.parseInt(myPort) / 2));

        try
        {
            //we need to know ids of every node in the system
            for(int i=0;i<ar.length;i++)
            {
                String nodeID=genHash(String.valueOf(Integer.parseInt(ar[i])/2));
                allPortsIDs.put(nodeID,ar[i]);
            }
            nodeIDs.addAll(allPortsIDs.keySet());
            Collections.sort(nodeIDs);
            int index=nodeIDs.indexOf(myID);
            // now we need successor ports for replication
            successor1=allPortsIDs.get(nodeIDs.get((index+1)%5));//get the successor one port
            successor2=allPortsIDs.get(nodeIDs.get((index+2)%5));//get the successor two port
            //we also need preID for checking the authority of the key
            preID=nodeIDs.get((index+4)%5);//get pre ID
            preID2=nodeIDs.get((index+3)%5);
            preID2=nodeIDs.get((index+2)%5);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (Exception e)
        {
            Log.d(TAG,"Error in onCreate"+e);
        }
        //now we need to get messages from successor
        Data getRecoveryData = new Data("","",Code.GETRECOVERY,successor1,myPort);
        Log.d(TAG,"Checking if there are any recovery messeges");
        new getRecoveryMessage().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, getRecoveryData);
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Cursor cursor=null;
        if(selection.equals("*"))
        {
            storage.clear();
            for(String port : ar)
            {
                Data getAllMsg = new Data("","",Code.GETALLMESSAGE,port,myPort);// we do not send any key or value
                new getAllMessages().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, getAllMsg);
            }
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MatrixCursor cursor2 = new MatrixCursor(new String[]{dbcontract.COLUMN_KEY, dbcontract.COLUMN_VALUE});
            for (Map.Entry mapElement : storage.entrySet()) {
                String key = (String) mapElement.getKey();
                String value = (String) mapElement.getValue();
                cursor2.addRow(new String[]{key, value});
            }
            return cursor2;
        }
        else if(selection.equals("@"))
        {
            SQLiteDatabase db = pa4DbHelper.getReadableDatabase();
            try {
                cursor = db.query(dbcontract.TABLE_NAME, null,null ,null , null, null, null, null);
            } catch (Exception e) {
                Log.d(TAG, "error in query=" + e);
            }
            return cursor;
        }
        else
        {
            Log.d("Queried key ", "=" + selection);
            SQLiteDatabase db = pa4DbHelper.getReadableDatabase();
            try {
                cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?" ,new String[]{selection} , null, null, null, null);
            } catch (Exception e) {
                Log.d("The error is in query", "=" + e);
            }

            if(cursor!=null && cursor.getCount()>0)
            {
                Log.d(TAG,"successful query");
                return cursor;
            }
            else
            {
                Log.d(TAG,"unnnnsuccessful query : "+selection);
                //we need to find coordinator which has the key
               // storage.clear();
                String coordinatorPort=findCoordinatorPort(genHash(selection)); //get the coordinator port
                Data getSingleMsg = new Data(selection,"",Code.GETSINGLEMESSAGE,coordinatorPort,myPort);// we do not send any key or value
                new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, getSingleMsg);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                MatrixCursor cursor2 = new MatrixCursor(new String[]{dbcontract.COLUMN_KEY, dbcontract.COLUMN_VALUE});
                String value = (String) storage.get(selection);
                cursor2.addRow(new String[]{selection, value});
                Log.d(TAG,"Returned value for key:"+selection+" is :"+storage);
                Log.d(TAG,"hashmap value : "+storage);
                return cursor2;
            }
        }
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) {
	    try
        {
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
            Log.d("Exception","in genhash function"+e);
        }
        return null;
    }
    private synchronized void sendRecoveryMessageToSucc(Data recvMsg)
    {
        new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, recvMsg);
    }
   /* private synchronized void sendRecoveryMessageToCord(Data recvMsg)
    {
        new SendMessageToSuccessor().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, recvMsg);
    }*/

    private boolean check(String keyhash) {

        if(preID.equalsIgnoreCase(myID)) {
            return true;
        }
        else if(myID.compareTo(preID)>0)
        {
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


    private String findCoordinatorPort(String keyhash)
    {
        String coordinator = "";

	    for(int i=0;i<nodeIDs.size();i++)
        {
            if(keyhash.compareTo(nodeIDs.get(i))<=0)
            {
                coordinator=nodeIDs.get(i);
                break;
            }
        }
        if(coordinator.equals(""))
        {
            coordinator=nodeIDs.get(0);
        }

        String port=allPortsIDs.get(coordinator);
        return port;
    }
    private String getSuccOfCord(String coPort)
    {
        /*1.first get the id of the coordinator using genhash()
        2.get the index of coordinator id in the nodeIDs list
        3. get the successor id from the nodeIDs list using coordinator index+1
        4.get successor port from the allPortsIDs hashmap  using successor id
        5.return the successor port
         */
        Log.d(TAG,"ARX co port"+coPort);
        String cordID=genHash(String.valueOf(Integer.parseInt(coPort)/2));

        int index=nodeIDs.indexOf(cordID);
        Log.d(TAG,"ARX index:"+index);
        String cordSuccID=nodeIDs.get((index+1)%5);

        String cordSuccPort=allPortsIDs.get(cordSuccID);
        Log.d(TAG,"ARX co succ port"+cordSuccPort);
        return cordSuccPort;
    }


    //client task
    private class SendMessageToSuccessor extends AsyncTask<Data, Void, Void> {

        @Override
        protected Void doInBackground(Data... args) {
            Socket socket;
            Data msgToSend = args[0];
            ObjectOutputStream objwriter=null;
            ObjectInputStream objReader=null;
            try {
                Log.d(TAG,"in send sending to port:"+msgToSend.DestinationPort);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToSend.DestinationPort));
                socket.setSoTimeout(800);
                Log.d(TAG,"Socket created soc:"+socket);
                objwriter= new ObjectOutputStream(socket.getOutputStream());
                objReader= new ObjectInputStream(socket.getInputStream());
                objwriter.writeObject(msgToSend);
                objwriter.flush();

                Data ackk=(Data) objReader.readObject();
                if(ackk.value.equals(ACK))
                {
                    socket.close();
                    objReader.close();
                    objwriter.close();
                }
            }
            catch (IOException e)
            {
                Log.d(TAG,"IO exception :"+msgToSend.DestinationPort + "  "+e);
                try
                {  if( msgToSend.code== Code.COORDINATOR)
                    {
                        String cosuccport=getSuccOfCord(msgToSend.DestinationPort);
                        Log.d(TAG,"sending key:"+msgToSend.key+"co:"+msgToSend.DestinationPort +" to succ:"+cosuccport);
                        msgToSend.code= Code.RECOVERYMSG;
                        msgToSend.DestinationPort=cosuccport;
                        Data sendRecovery = new Data(msgToSend.key,msgToSend.value,Code.RECOVERYMSG,cosuccport,myPort);
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(cosuccport));
                        socket2.setSoTimeout(800);
                        objwriter= new ObjectOutputStream(socket2.getOutputStream());
                        objReader= new ObjectInputStream(socket2.getInputStream());
                        objwriter.writeObject(sendRecovery);
                        objwriter.flush();
                        Data ackk=(Data) objReader.readObject();
                        if(ackk.value.equals(ACK))
                        {   Log.d(TAG,"ACK received");
                            socket2.close();
                            objReader.close();
                            objwriter.close();
                        }
                        Log.d(TAG,"Message sent to successor successfully ");
                    }
                    else if(msgToSend.code==Code.REPLICATION)
                    {
                        String cosuccport=getSuccOfCord(msgToSend.DestinationPort);
                        Log.d(TAG,"sending replication key:"+msgToSend.key+"co:"+msgToSend.DestinationPort +" to succ:"+cosuccport);
                        Data sendRecovery = new Data(msgToSend.key,msgToSend.value,Code.REPLICATIONRECOVERY,cosuccport,myPort);
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(cosuccport));
                        socket2.setSoTimeout(800);
                        objwriter= new ObjectOutputStream(socket2.getOutputStream());
                        objReader= new ObjectInputStream(socket2.getInputStream());
                        objwriter.writeObject(sendRecovery);
                        objwriter.flush();
                        Data ackk=(Data) objReader.readObject();
                        if(ackk.value.equals(ACK))
                        {   Log.d(TAG,"ACK received");
                            socket2.close();
                            objReader.close();
                            objwriter.close();
                        }
                        Log.d(TAG,"Message sent to successor successfully ");
                    }
                    else if(msgToSend.code==Code.GETSINGLEMESSAGE)
                    {
                        String cosuccport=getSuccOfCord(msgToSend.DestinationPort);
                        Log.d(TAG,"sending query of:"+msgToSend.DestinationPort +" to succ:"+cosuccport);
                        Data sendRecovery = new Data(msgToSend.key,msgToSend.value,Code.GETSINGLEMESSAGE,cosuccport,myPort);
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(cosuccport));
                        socket2.setSoTimeout(800);
                        objwriter= new ObjectOutputStream(socket2.getOutputStream());
                        objReader= new ObjectInputStream(socket2.getInputStream());
                        objwriter.writeObject(sendRecovery);
                        objwriter.flush();
                        Data ackk=(Data) objReader.readObject();
                        if(ackk.value.equals(ACK))
                        {   Log.d(TAG,"ACK received");
                            socket2.close();
                            objReader.close();
                            objwriter.close();
                        }
                        Log.d(TAG,"Message sent to successor successfully ");
                    }
                }
                catch (Exception E)
                {
                    Log.d(TAG,"failure while sending cordinator message to successor"+e);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    private class getRecoveryMessage extends AsyncTask<Data, Void, Void> {

        @Override
        protected Void doInBackground(Data... args) {
            Socket socket;
            Data msgToSend = args[0];
            ObjectOutputStream objwriter = null;
            ObjectInputStream objReader = null;
            try {
                Log.d(TAG, "in send sending to port:" + msgToSend.DestinationPort);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToSend.DestinationPort));
                socket.setSoTimeout(800);
                Log.d(TAG, "Socket created soc:" + socket);
                objwriter = new ObjectOutputStream(socket.getOutputStream());
                objReader = new ObjectInputStream(socket.getInputStream());
                objwriter.writeObject(msgToSend);
                objwriter.flush();

                Data ackk = (Data) objReader.readObject();
                while(!ackk.value.equals(ACK))
                {
                    ContentValues cv= new ContentValues();
                    cv.put(dbcontract.COLUMN_KEY,ackk.key);
                    cv.put(dbcontract.COLUMN_VALUE,ackk.value);
                    insert(null,cv);
                    ackk=(Data) objReader.readObject();

                }
                if (ackk.value.equals(ACK)) {
                    socket.close();
                    objReader.close();
                    objwriter.close();
                }
            } catch (IOException e) {

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class getAllMessages extends AsyncTask<Data, Void, Void> {

        @Override
        protected Void doInBackground(Data... args) {
            Socket socket;
            Data msgToSend = args[0];
            ObjectOutputStream objwriter = null;
            ObjectInputStream objReader = null;
            try {
                Log.d(TAG, "in send sending to port:" + msgToSend.DestinationPort);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToSend.DestinationPort));
                socket.setSoTimeout(800);
                Log.d(TAG, "Socket created soc:" + socket);
                objwriter = new ObjectOutputStream(socket.getOutputStream());
                objReader = new ObjectInputStream(socket.getInputStream());
                objwriter.writeObject(msgToSend);
                objwriter.flush();

                Data ackk = (Data) objReader.readObject();
                while(!ackk.value.equals(ACK))
                {
                    synchronized (storage)
                    {
                        storage.put(ackk.key,ackk.value);
                    }
                    ackk=(Data) objReader.readObject();
                }
                if (ackk.value.equals(ACK)) {
                    socket.close();
                    objReader.close();
                    objwriter.close();
                }
            } catch (IOException e) {

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    //server task
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket soc=null;
            ObjectInputStream objin=null;
            ObjectOutputStream objwriter=null;
            Data ack=null;
            while(!isCancelled()) {
                try {
                    soc = serverSocket.accept();
                    objin = new ObjectInputStream(soc.getInputStream());
                    objwriter= new ObjectOutputStream(soc.getOutputStream());
                    Data readData=null;
                    try
                    {
                      readData = (Data)objin.readObject();
                    }
                    catch (Exception e)
                    {
                        Log.d(TAG,"error while reading data in server"+e);
                    }

                    if(readData == null)
                    {
                        //do nothing
                    }
                    else
                    {
                        switch (readData.code){
                            case REPLICATION:
                                ContentValues storeReplica = new ContentValues();
                                storeReplica.put(dbcontract.COLUMN_KEY,readData.key);
                                storeReplica.put(dbcontract.COLUMN_VALUE,readData.value);
                                insert(null, storeReplica);
                                Log.d(TAG,"Message replicated : msg from "+readData.SourcePort);
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case COORDINATOR:
                                Log.d(TAG,"Received message from"+readData.SourcePort);
                                ContentValues storeValue = new ContentValues();
                                storeValue.put(dbcontract.COLUMN_KEY,readData.key);
                                storeValue.put(dbcontract.COLUMN_VALUE,readData.value);
                                insert(Uri.parse("abs"), storeValue);
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case GETALLMESSAGE:
                                Cursor allMessages=query(null,null,"@",null,null);
                                while (allMessages.moveToNext())
                                {
                                    String tempKey=allMessages.getString(allMessages.getColumnIndex(dbcontract.COLUMN_KEY));
                                    String tempValue =allMessages.getString(allMessages.getColumnIndex(dbcontract.COLUMN_VALUE));
                                    Data temp = new Data(tempKey,tempValue, Code.RETURNALLMSGS, readData.SourcePort, myPort);
                                    objwriter.writeObject(temp);
                                }
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case GETSINGLEMESSAGE:
                                Cursor singleMsg=query(null,null,readData.key,null,null);
                                while (singleMsg.moveToNext())
                                {
                                    String tempKey=singleMsg.getString(singleMsg.getColumnIndex(dbcontract.COLUMN_KEY));
                                    String tempValue =singleMsg.getString(singleMsg.getColumnIndex(dbcontract.COLUMN_VALUE));
                                    Data temp = new Data(tempKey,tempValue, Code.RETURNSINGLEMSG, readData.SourcePort, myPort);
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(readData.SourcePort));
                                    ObjectOutputStream objwriter3= new ObjectOutputStream(socket.getOutputStream());
                                    objwriter3.writeObject(temp);
                                    objwriter3.flush();
                                }
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case RETURNALLMSGS:
                                synchronized(storage) {
                                    storage.put(readData.key,readData.value);
                                }
                                break;
                            case RETURNSINGLEMSG:
                                synchronized(storage) {
                                    storage.put(readData.key,readData.value);
                                }
                                break;
                            case DELETE:
                                delete(null,"*",null);
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case RECOVERYMSG:

                                Log.d(TAG,"Storing the recovery message for predecessor="+readData.key +" from ="+readData.SourcePort);
                                ContentValues storeReplica2 = new ContentValues();
                                storeReplica2.put(dbcontract.COLUMN_KEY,readData.key);
                                storeReplica2.put(dbcontract.COLUMN_VALUE,readData.value);
                                insert(null, storeReplica2);
                                synchronized (recovery)
                                {
                                    recovery.put(readData.key,readData.value);
                                }
                                // now we need to send this message to our successor so that it also store this value
                                Data sendTosucc =new Data(readData.key,readData.value,Code.REPLICATION,successor1,myPort);
                                sendRecoveryMessageToSucc(sendTosucc);
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case REPLICATIONRECOVERY:
                                synchronized (recovery)
                                {
                                    recovery.put(readData.key,readData.value);
                                }
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                            case GETRECOVERY:
                                Log.d(TAG,"Recovery messege from "+readData.SourcePort+ " size of hashmap="+recovery.size());

                                if(recovery.size()!=0)
                                {
                                    for (Map.Entry mapElement : recovery.entrySet()) {
                                        String tempkey=(String) mapElement.getKey();
                                        String tempValue=(String) mapElement.getValue();
                                        Log.d(TAG,"inside recovery key="+tempkey +" value="+tempValue);
                                        Data temp= new Data(tempkey,tempValue,Code.RECOVERYFROMTHESUCC,readData.SourcePort,myPort);
                                        objwriter.writeObject(temp);
                                    }
                                }
                                recovery.clear();
                                ack =  new Data("",ACK,Code.ACK,readData.SourcePort,myPort);
                                objwriter.writeObject(ack);
                                break;
                       /*     case RECOVERYFROMTHESUCC:
                                synchronized (recovery)
                                {
                                    Log.d(TAG, "inside RECOVERYFROMSUCC key="+readData.key);
                                    ContentValues recInsrt= new ContentValues();
                                    recInsrt.put(dbcontract.COLUMN_KEY,readData.key);
                                    recInsrt.put(dbcontract.COLUMN_VALUE,readData.value);
                                    insert(null,recInsrt);
                                }
                                break;*/
                            default:
                                break;

                        }
                    }

                } catch (Exception e) {
                    Log.d(TAG,"Error in server side"+e);
                }
            }
            return null;
        }
    }
}

