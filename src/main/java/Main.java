package main.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import main.java.transaction.*;

public class Main {

    public static final String HOST_KEY = "HOST";
    public static final String PORT_KEY = "PORT";
    public static final String DATABASE_KEY = "DATABASE";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Format: \"[local or majority] [number of clients]\"");
            System.out.println("Example: local 10");
            System.exit(1);
        }

        String consistencyLevel = args[0];
        int numTransactions = Integer.parseInt(args[1]);

        Triple<String, String, String> configData = readConfigFile();

        String host = configData.first;
        int port = Integer.parseInt(configData.second);
        String databaseName = configData.third;

        ExecutorService executorService = Executors.newFixedThreadPool(Math.max(1, numTransactions));
        List<Future<Triple<Integer, Long, Double>>> futureMeasurements =
                new ArrayList<Future<Triple<Integer, Long, Double>>>();

        for (int i = 1; i < numTransactions; i++) {
            Future<Triple<Integer, Long, Double>> future =
                    executorService.submit(new ClientThread(i, consistencyLevel, host, port, databaseName));
            futureMeasurements.add(future);
        }

        Map<Integer, Triple<Integer, Long, Double>> measurementMap =
                new HashMap<Integer, Triple<Integer, Long, Double>>();
        for (Future<Triple<Integer, Long, Double>> future : futureMeasurements) {
            try {
                Triple<Integer, Long, Double> tuple = future.get();
                measurementMap.put(tuple.first, tuple);
            } catch (InterruptedException e){
                e.printStackTrace();
                return;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private static Triple<String, String, String> readConfigFile() {
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

        return new Triple<String, String, String>(host, port, databaseName);
    }
}