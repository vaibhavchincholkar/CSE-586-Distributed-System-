package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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

    String failedport="";
    String[] ar ={REMOTE_PORT0,REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    //Collections.synchronizedList makes list thread safe
    List<HoldBackMessage> HoldBL = Collections.synchronizedList(new ArrayList<HoldBackMessage>());

    private int finalseq=0;
    private int agr_seq=-1;
    private int pro_seq=-1;

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
                        if(msg.contains("*"))
                        {
                            int index=msg.indexOf('*');
                            String new_msg=msg.substring(0,index);
                            String port=msg.substring(index+1);
                            Log.d("port",":"+port);
                            Log.d("msg is",":"+new_msg);
                            pro_seq=(Math.max(pro_seq,agr_seq)+1); // increment the proposed number

                            HoldBL.add(new HoldBackMessage(new_msg,port,pro_seq,false));//store message with proposed number
                            pro_sort_list(HoldBL);//sort the list according to the proposed number
                            out.print(Integer.toString(pro_seq));//send the proposed number
                            out.flush();
                        }
                        else
                        {
                            int index = msg.indexOf('=');
                            String port_id = msg.substring(0, index);//get the port from the message sent
                            String cagr_seq = msg.substring(index + 1);//get the agreed seq number
                            agr_seq = Integer.parseInt(cagr_seq);//store it in agr_seq

                            //find the port from the Hold back queue
                            for (HoldBackMessage hbmsg : HoldBL)
                            {
                                if (hbmsg.port.equals(port_id))
                                {
                                    hbmsg.deliverable=true;
                                    hbmsg.tempseq=agr_seq;
                                    Log.d("Agreed seq no for ",hbmsg.message+" is "+agr_seq);
                                    break;
                                }
                            }
                            //delete failed port's messages if there is any
                            delete_failed_avd_msg( HoldBL);

                            // now again sort the list using new agreed seq no
                            sort_list(HoldBL);

                            List<HoldBackMessage> tempHoldBL = Collections.synchronizedList(new ArrayList<HoldBackMessage>());

                            for(int i=0;i<HoldBL.size();i++)
                            {
                                HoldBackMessage temp=HoldBL.get(i);
                                if(temp.deliverable==true)
                                {
                                    Log.d("Inserting into db","for port number"+port_id);
                                    ContentValues insert_msg = new ContentValues();
                                    insert_msg.put(dbcontract.COLUMN_KEY,Integer.toString(finalseq) );
                                    insert_msg.put(dbcontract.COLUMN_VALUE, temp.message);
                                    mycontentresolver.insert(muri, insert_msg);
                                    tempHoldBL.add(temp);
                                    finalseq=finalseq+1;
                                }
                                else
                                {
                                    Log.d("Not ture","inside holb back queue traversal");
                                    break;
                                }
                            }

                            //now delete the delivered messages from the queue
                            for(int t=0;t<tempHoldBL.size();t++)
                            {
                                HoldBL.remove(tempHoldBL.get(t));
                            }

                            out.print("ack");//send ack to close the socket
                            out.flush();
                        }
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

    private synchronized void delete_failed_avd_msg(List<HoldBackMessage> HoldBL)
    {
        if(!failedport.equals(""))
        {
            for(int i=0;i<HoldBL.size();i++)
            {
                HoldBackMessage HBmsg_temp=HoldBL.get(i);
                if((Integer.parseInt(HBmsg_temp.port)==Integer.parseInt(failedport)))
                {
                    Log.d("deleting the port",""+HBmsg_temp.port);
                    HoldBL.remove(HBmsg_temp);
                }
            }
        }

    }

    private synchronized void pro_sort_list( List<HoldBackMessage> HoldBL) //it sorts the list
    {

        for (int i = 0; i < HoldBL.size()-1; i++)
        {
            for (int j = 0; j < HoldBL.size()-i-1; j++)
            {
                HoldBackMessage HBmsg_temp=HoldBL.get(j);
                HoldBackMessage HBmsg_temp2=HoldBL.get(j+1);

                if(HBmsg_temp.tempseq>HBmsg_temp2.tempseq)
                {
                    HoldBL.set(j,HBmsg_temp2);
                    HoldBL.set(j+1,HBmsg_temp);
                }
                if(HBmsg_temp.tempseq==HBmsg_temp2.tempseq)
                {
                    if(Integer.parseInt(HBmsg_temp.port)>Integer.parseInt(HBmsg_temp2.port))
                    {
                        HoldBL.set(j,HBmsg_temp2);
                        HoldBL.set(j+1,HBmsg_temp);
                    }
                }
            }
        }
    }


    private synchronized void sort_list( List<HoldBackMessage> HoldBL)
    {


        for (int i = 0; i < HoldBL.size()-1; i++)
        {
            for (int j = 0; j < HoldBL.size()-i-1; j++)
            {
                HoldBackMessage HBmsg_temp=HoldBL.get(j);
                HoldBackMessage HBmsg_temp2=HoldBL.get(j+1);

                if(HBmsg_temp.tempseq>HBmsg_temp2.tempseq)
                {
                    HoldBL.set(j,HBmsg_temp2);
                    HoldBL.set(j+1,HBmsg_temp);
                }
                if(HBmsg_temp.tempseq==HBmsg_temp2.tempseq)
                {
                    if(Integer.parseInt(HBmsg_temp.port)>Integer.parseInt(HBmsg_temp2.port))
                    {
                        HoldBL.set(j,HBmsg_temp2);
                        HoldBL.set(j+1,HBmsg_temp);
                    }
                }
            }
        }
        Log.d("Message in queue is","================================================================================================");
        for(int i=0;i<HoldBL.size();i++)
        {
            Log.d("Delivery queue","seq no:"+HoldBL.get(i).tempseq+" Port:"+HoldBL.get(i).port +" Msg:"+HoldBL.get(i).message);
        }
        Log.d("Message in end is","================================================================================================");
    }

    //client side
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
          /*  Socket socket0, socket1, socket2, socket3, socket4;
            String remotePort = msgs[1];
            String msgToSend = msgs[0];
          // msgToSend.replaceAll("\\r|\\n","");
            msgToSend=msgToSend.substring(0,msgToSend.length()-1)+"*"+myPort+"\n";
           // Log.d("sender"," new msg is :"+msgToSend);
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

                    Log.d("reply",""+ack0);
                    Log.d("reply",""+ack1);
                    Log.d("reply",""+ack2);
                    Log.d("reply",""+ack3);
                    Log.d("reply",""+ack4);

                    List<Integer> temp=new ArrayList<>(10);

                    if(ack0!=null)
                    {
                        temp.add(Integer.parseInt(ack0));
                    }
                    if(ack1!=null)
                    {
                        temp.add(Integer.parseInt(ack1));
                    }
                    if(ack2!=null)
                    {
                        temp.add(Integer.parseInt(ack2));
                    }
                    if(ack3!=null)
                    {
                        temp.add(Integer.parseInt(ack3));
                    }
                    if(ack4!=null)
                    {
                        temp.add(Integer.parseInt(ack4));
                    }

                    int max_agr_no=Collections.max(temp);

                    String second_msg=myPort+"="+Integer.toString(max_agr_no)+"\n";
                    Log.d("second msg",""+second_msg+" msg is:"+msgToSend);

                    out0.close();
                    out1.close();
                    out2.close();
                    out3.close();
                    out4.close();
                    socket0.close();
                    socket1.close();
                    socket2.close();
                    socket3.close();
                    socket4.close();

                    socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[0]));
                    socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[1]));
                    socket2= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[2]));
                    socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[3]));
                    socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[4]));

                    out0 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket0.getOutputStream())), true);
                    out1= new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())), true);
                    out2 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream())), true);
                    out3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())), true);
                    out4 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket4.getOutputStream())), true);
                    out0.print(second_msg);
                    out0.flush();
                    out1.print(second_msg);
                    out1.flush();
                    out2.print(second_msg);
                    out2.flush();
                    out3.print(second_msg);
                    out3.flush();
                    out4.print(second_msg);
                    out4.flush();

                    if(ack0.equals("ack"))
                    {
                        socket0.close();
                    }
                    if(ack1.equals("ack"))
                    {
                        socket1.close();
                    }
                    if(ack2.equals("ack"))
                    {
                        socket2.close();
                    }
                    if(ack3.equals("ack"))
                    {
                        socket3.close();
                    }
                    if(ack4.equals("ack"))
                    {
                        socket4.close();
                    }

                }
                catch (NullPointerException e) {

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

            return null;*/





          //
            String remotePort = msgs[1];
            String msgToSend = msgs[0];
            Socket socket, socket2;
            msgToSend=msgToSend.substring(0,msgToSend.length()-1)+"*"+myPort+"\n";
            //sending first message to get the aggred seq
            List<Integer> temp=new ArrayList<>(10);// to hold the aggred numbes from all the avds
            int max_agr_no=0;
            for(int i=0;i<ar.length; i++)
            {
                try
                {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[i]));
                    //socket.setSoTimeout(500);
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
                    String ack;
                    ack=input.readLine();
                    if(ack!=null)
                    {
                        temp.add(Integer.parseInt(ack));
                    }
                    out.close();
                    socket.close();
                }
                catch (UnknownHostException e)
                {
                    Log.e("error", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    failedport=ar[i];
                    Log.e("error", "ClientTask socket IOException");
                }
                catch (Exception e)
                {
                    Log.e("error in first", "port is"+ar[i]+"error is"+e);
                }

            }

            max_agr_no=Collections.max(temp);//we got the largest message
            String second_msg=myPort+"="+Integer.toString(max_agr_no)+"\n";
            Log.d("second msg",""+second_msg+" msg is:"+msgToSend);

            for(int i=0;i<ar.length; i++)
            {
                try
                {
                    socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ar[i]));
                    socket2.setSoTimeout(800);
                    PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream())), true);
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket2.getInputStream()));

                    out.print(second_msg);
                    out.flush();
                    String ack;

                    ack=input.readLine();

                    if(ack.equals("ack"))
                    {
                        socket2.close();
                    }
                }
                catch (UnknownHostException e)
                {
                    Log.e("error", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    failedport=ar[i];
                    Log.e("error", "ClientTask socket IOException"+"port is :"+ar[i]);
                }
                catch (Exception e)
                {
                    Log.e("error second", "port is"+ar[i]+"error is"+e);
                }

            }

          return null;
        }
    }
}
