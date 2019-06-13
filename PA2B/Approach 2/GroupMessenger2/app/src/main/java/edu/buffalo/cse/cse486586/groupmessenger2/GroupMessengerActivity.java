package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    private Button send;
    private EditText value;
    Uri muri;
    String myPort;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String ACK_MSG="ACK";
    String[] ar ={REMOTE_PORT0,REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    List<DelieveryQueueMessage> DeliveryQ =Collections.synchronizedList(new ArrayList<DelieveryQueueMessage>());
    private int seq=0;

    String msg="";
    private ContentResolver mycontentresolver = null;
    static final int SERVER_PORT = 10000;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //get port
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort  = String.valueOf((Integer.parseInt(portStr) * 2));

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        value=(EditText) findViewById(R.id.editText1);

        //get content resolver
        mycontentresolver=getContentResolver();

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(new OnPTestClickListener(tv, getContentResolver()));

        try
        {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (Exception e)
        {
            Log.e("socket error",""+e);
        }

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.scheme("content");
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
        muri=uriBuilder.build();

        //Delete everything when activity starts
        mycontentresolver.delete(muri,null,null);

        send=(Button)findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener()
        {
            public void onClick(View v) {
                msg = value.getText().toString() + "\n";
                value.setText(""); // This is one way to reset the input box.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected synchronized Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader input = null;
            PrintWriter out=null;
            Socket soc=null;
            String msg = "";

            while(!isCancelled())
            {
                try
                {
                    soc = serverSocket.accept();//Listens for a connection to be made to this socket and accepts it. The method blocks until a connection is made.
                    input = new BufferedReader(new InputStreamReader(soc.getInputStream()));
                    msg = input.readLine();

                    if(msg.equals("")||msg.isEmpty())
                    {
                        //dont do anything
                    }
                    else
                    {
                        out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(soc.getOutputStream())),true);
                        //get the port number out

                        int index=msg.indexOf('*');
                        String new_msg=msg.substring(0,index);
                        String port=msg.substring(index+1);
                        Log.d("port",":"+port);
                        Log.d("msg is",":"+new_msg);

                        //Add new message in the list
                        DeliveryQ.add(new DelieveryQueueMessage(seq, port, new_msg));
                        // increment the sequence counter
                        seq=+1;
                        //sort the list with the new added message
                        List<DelieveryQueueMessage> SortedQ = sort_list(DeliveryQ);
                        //delete everything from the content provider
                        mycontentresolver.delete(muri, null, null);

                        //now insert everything in sorted order
                        for (int i = 0; i < SortedQ.size(); i++)
                        {
                            ContentValues insert_msg = new ContentValues();
                            insert_msg.put(dbcontract.COLUMN_KEY, Integer.toString(i));
                            insert_msg.put(dbcontract.COLUMN_VALUE, SortedQ.get(i).DMMsgToInsert);
                            mycontentresolver.insert(muri, insert_msg);
                        }
                        //
                        out.print(ACK_MSG);
                        out.flush();

                        publishProgress(msg);
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
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived+"\n");
            return;
        }
    }

    //
    private List<DelieveryQueueMessage> sort_list(List<DelieveryQueueMessage> DeliveryQ)
    {
        //simple bubble sort to sort the list before insertion
        for (int i = 0; i < DeliveryQ.size()-1; i++)
        {
            for (int j = 0; j < DeliveryQ.size()-i-1; j++)
            {
                DelieveryQueueMessage dqmsg_temp=DeliveryQ.get(j);
                DelieveryQueueMessage dqmsg_temp2=DeliveryQ.get(j+1);
                if(Integer.parseInt(dqmsg_temp.DMport)>Integer.parseInt(dqmsg_temp2.DMport))
                {
                    DeliveryQ.set(j, dqmsg_temp2);
                    DeliveryQ.set(j + 1, dqmsg_temp);
                }
            }
        }
        return DeliveryQ;
    }

    //client side
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket0, socket1, socket2, socket3, socket4;
            String remotePort = msgs[1];
            String msgToSend = msgs[0];
            msgToSend=msgToSend.substring(0,msgToSend.length()-1)+"*"+myPort+"\n";
            try
            {
                
                socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[0]));
                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[1]));
                socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[2]));
                socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[3]));
                socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[4]));

                PrintWriter out0 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket0.getOutputStream())), true);
                PrintWriter out1 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())), true);
                PrintWriter out2 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream())), true);
                PrintWriter out3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())), true);
                PrintWriter out4 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket4.getOutputStream())), true);

                BufferedReader input0 = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
                BufferedReader input1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                BufferedReader input2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
                BufferedReader input3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
                BufferedReader input4 = new BufferedReader(new InputStreamReader(socket4.getInputStream()));

                if(msgToSend.isEmpty()||msgToSend.equals("null")||msgToSend.equals(""))
                {
                    //do nothing
                }
                else
                {
                    out0.print(msgToSend);
                    out0.flush();
                    out1.print(msgToSend);
                    out1.flush();
                    out2.print(msgToSend);
                    out2.flush();
                    out3.print(msgToSend);
                    out3.flush();
                    out4.print(msgToSend);
                    out4.flush();
                }

                String ack0,ack1,ack2,ack3,ack4;

                try
                {
                    ack0=input0.readLine();
                    ack1=input1.readLine();
                    ack2=input2.readLine();
                    ack3=input3.readLine();
                    ack4=input4.readLine();

                    if(ack0.equals(ACK_MSG))
                    {
                        socket0.close();
                    }
                    if(ack1.equals(ACK_MSG))
                    {
                        socket1.close();
                    }
                    if(ack2.equals(ACK_MSG))
                    {
                        socket2.close();
                    }
                    if(ack3.equals(ACK_MSG))
                    {
                        socket3.close();
                    }
                    if(ack4.equals(ACK_MSG))
                    {
                        socket4.close();
                    }

                }
                catch (Exception e)
                {
                    Log.d("Exception at sender", ""+e);
                }
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
}
