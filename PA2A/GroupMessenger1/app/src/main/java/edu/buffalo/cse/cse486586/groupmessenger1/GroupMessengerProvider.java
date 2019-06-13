package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
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
        return true;
    }
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
         final SQLiteDatabase db = pa2DbHelper.getWritableDatabase();
            db.beginTransaction();

            long insrt=-1;
            ContentValues change = new ContentValues();;
            change.put(dbcontract.COLUMN_KEY,values.getAsString("key"));
            change.put(dbcontract.COLUMN_VALUE,values.getAsString("value"));
            if(CheckIfContains(values.getAsString("key")) )
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

                Uri uuri = ContentUris.withAppendedId(uri, insrt);
                getContext().getContentResolver().notifyChange(uuri, null);
                db.setTransactionSuccessful();
                db.endTransaction();
                Log.d("pa2insert",""+values.getAsString("value"));
                return uuri;
            }

       //  Log.v("insert", values.toString());
       // return uri;
    }

    public boolean CheckIfContains(String key)
    {
        Log.d("msg","inside check:"+key);
        final SQLiteDatabase db = pa2DbHelper.getReadableDatabase();
        try
        {

            //String Query = "Select * from " + dbcontract.TABLE_NAME + " where '" + dbcontract.COLUMN_KEY + "' = '" + key+"';";
            //Cursor cursor =db.rawQuery(Query, null);
            Cursor cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?", new String[]{key},null,null,null);

            if(cursor.getCount() <= 0)
            {
                Log.d("msg","returend false");
                cursor.close();
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

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        final SQLiteDatabase db = pa2DbHelper.getReadableDatabase();
        Cursor cursor = db.query(dbcontract.TABLE_NAME, null,dbcontract.COLUMN_KEY + " = ?", new String[]{selection},null,null,null);
        return cursor;
    }
}
