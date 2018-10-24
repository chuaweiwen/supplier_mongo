package main.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import main.java.transaction.*;

public class Main {

    public static final String HOST_KEY = "HOST";
    public static final String PORT_KEY = "PORT";
    public static final String DATABASE_KEY = "DATABASE";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Format: \"[concern] [size]\"");
            System.out.println("Example: local 10");
            System.exit(1);
        }

        String concern = args[0];
        int numTransactions = Integer.parseInt(args[1]);

        Triple<String> configData = readConfigFile();

        String host = configData.first();
        int port = Integer.parseInt(configData.second());
        String databaseName = configData.third();
    }

    private static Triple<String> readConfigFile() {
        String host = Constant.DEFAULT_HOST;
        String port = Constant.DEFAULT_PORT;
        String databaseName = Constant.DEFAULT_DATABASE;

        String line;
        try {
            FileReader fr = new FileReader(Constant.CONFIGURATION_FILE);
            BufferedReader br = new BufferedReader(fr);

            while ((line = br.readLine()) != null) {
                String[] lineValue = line.split("=");
                if (lineValue[0] == HOST_KEY) {
                    host = lineValue[1];
                } else if (lineValue[0] == PORT_KEY) {
                    port = lineValue[1];
                } else if (lineValue[0] == DATABASE_KEY) {
                    databaseName = lineValue[1];
                }
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.out.println("Error reading " + Constant.CONFIGURATION_FILE);
            e.printStackTrace();
        }

        return new Triple(host, port, databaseName);
    }
}

class Triple<T> {
    private T first;
    private T second;
    private T third;

    public Triple(T first, T second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public T first() {
        return this.first;
    }

    public T second() {
        return this.second;
    }

    public T third() {
        return this.third;
    }
}