package main.java;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class Setup {

    private static final String HOST_NAME = "localhost";
    private static final String DATABASE_NAME = "supplier";

    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private DB db;

    public static void main(String args[]) {
        Setup s = new Setup();
        s.run();
    }

    public void run() {
        MongoClient client = new MongoClient(HOST_NAME);
    }
}