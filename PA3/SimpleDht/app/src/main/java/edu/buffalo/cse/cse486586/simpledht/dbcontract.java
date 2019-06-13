package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

public class dbcontract implements BaseColumns {
    //Table name
    public static final String TABLE_NAME="keyvalue";

    //Column names
    public static final String COLUMN_KEY="key";
    public static final String COLUMN_VALUE="value";
}
