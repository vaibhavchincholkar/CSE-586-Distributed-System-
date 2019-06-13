package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class PA2DbHelper extends SQLiteOpenHelper {
    public static final String DATABASE_NAME="PAtwo.db";
    private static final int DATABASE_VERSION=1;

    public PA2DbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }


    @Override
    public void onCreate(SQLiteDatabase db) {

          final String SQL_CREATE_ENTRIES =
                "CREATE TABLE " + dbcontract.TABLE_NAME + " (" +
                        dbcontract._ID + "INTEGER PRIMARY KEY," +
                        dbcontract.COLUMN_KEY + "TEXT," +
                        dbcontract.COLUMN_VALUE + " TEXT)";
        final String SQL_CREATE_TABLE= "CREATE TABLE " + dbcontract.TABLE_NAME+"("+dbcontract.COLUMN_KEY+"TEXT PRIMARY KEY,"+dbcontract.COLUMN_VALUE+"TEXT);";
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + dbcontract.TABLE_NAME);
        onCreate(db);
    }
}
