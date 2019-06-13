package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    private ContentResolver mycontentresolver = null;
    Uri muri;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        mycontentresolver=getContentResolver();
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.scheme("content");
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        muri=uriBuilder.build();
        mycontentresolver.delete(muri,"@",null);
        Button ldump=(Button) findViewById(R.id.button1);
        Button gdump=(Button) findViewById(R.id.button2);



        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        ldump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv.setText("");
                Cursor resultCursor=    mycontentresolver.query(muri,null,"@",null,null);
                while (resultCursor.moveToNext()) {
                    String key=resultCursor.getString(resultCursor.getColumnIndex(dbcontract.COLUMN_KEY));
                    String Value=resultCursor.getString(resultCursor.getColumnIndex(dbcontract.COLUMN_VALUE));

                    tv.append(key+" = "+Value+"\n");
                }
            }
        });

        gdump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv.setText("");
                Cursor resultCursor=    mycontentresolver.query(muri,null,"*",null,null);
                while (resultCursor.moveToNext()) {
                    String key=resultCursor.getString(resultCursor.getColumnIndex(dbcontract.COLUMN_KEY));
                    String Value=resultCursor.getString(resultCursor.getColumnIndex(dbcontract.COLUMN_VALUE));

                    tv.append(key+" = "+Value+"\n");
                }
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
