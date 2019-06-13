package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {
    private PA2DbHelper pa2DbHelper;
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        final SQLiteDatabase db = pa2DbHelper.getWritableDatabase();
        db.delete(dbcontract.TABLE_NAME,null,null);
        db.close();
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        pa2DbHelper= new PA2DbHelper(getContext());
        return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        long insrt=-1;
        ContentValues change = new ContentValues();
        change.put(dbcontract.COLUMN_KEY,values.getAsString(dbcontract.COLUMN_KEY));
        change.put(dbcontract.COLUMN_VALUE,values.getAsString(dbcontract.COLUMN_VALUE));
        boolean contains=CheckIfContains(values.getAsString("key"));

        final SQLiteDatabase db = pa2DbHelper.getWritableDatabase();
        db.beginTransaction();

        if(contains)
        {

            String strFilter = values.getAsString("key") ;
            insrt=db.update(dbcontract.TABLE_NAME,change,dbcontract.COLUMN_KEY + " = ?", new String[]{strFilter});
        }
        else
        {
            insrt=db.insert(dbcontract.TABLE_NAME,null,change);
        }
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

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Cursor cursor=null;
        final SQLiteDatabase db = pa2DbHelper.getReadableDatabase();
        try
        {
          //  cursor = db.query(dbcontract.TABLE_NAME, null,selection + " = ?", selectionArgs,null,null,null);
            cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?", new String[]{selection},null,null,null);
        }
        catch (Exception e)
        {
            Log.d("error in provider",""+e);
        }
        return cursor;
    }
    public boolean CheckIfContains(String key)
    {
        final SQLiteDatabase db = pa2DbHelper.getReadableDatabase();
        try
        {
            Cursor cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?", new String[]{key},null,null,null);
            if(cursor.getCount() <= 0)
            {
                Log.d("msg","returend false");
                return false;
            }
            Log.d("msg","returend true");
            cursor.close();
            return true;
        }
        catch (Exception e)
        {
            Log.d("msg","error"+e);
        }
        return false;
    }
}
