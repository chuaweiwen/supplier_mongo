package main.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) {
        String[] configData = readConfigFile();

        String host = configData[0];
        int port = Integer.parseInt(configData[1]);
        String databaseName = configData[2];
        String consistencyLevel = configData[3];
        int numTransactions = Integer.parseInt(configData[4]);

        try {
            BufferedWriter out = new BufferedWriter(
                    new FileWriter(databaseName + ".log", true));
            Date date = new Date();
            out.write(date + " Connecting to " + host + ":" + port + "\n");
            out.write(date + " Database: " + databaseName + "\n");
            out.write(date + " Begin experiment " + consistencyLevel + " " + numTransactions + "\n");
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long startTime = System.nanoTime();

        ExecutorService executorService = Executors.newFixedThreadPool(Math.max(1, numTransactions));
        List<Future<ClientStatistics>> results = new ArrayList<Future<ClientStatistics>>();

        for (int i = 1; i <= numTransactions; i++) {
            Future<ClientStatistics> future =
                    executorService.submit(new ClientThread(i, consistencyLevel, host, port, databaseName));
            results.add(future);
        }

        Map<Integer, ClientStatistics> statisticsMap = new HashMap<Integer, ClientStatistics>();
        int numXactError = 0;
        for (Future<ClientStatistics> future : results) {
            try {
                ClientStatistics statistics = future.get();
                statisticsMap.put(statistics.getIndex(), statistics);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            } catch (ExecutionException e) {
                e.printStackTrace();
                numXactError++;
            }
        }

        outputPerformanceResults(statisticsMap, consistencyLevel, numTransactions);
        System.out.println("\nAll " + numTransactions + " clients have completed their transactions.");
        System.out.println("Number of transactions with error: " + numXactError);

        try {
            long endTime = System.nanoTime() - startTime;
            double endTimeInSeconds = endTime / 1000000000.0;
            System.out.println("Total time taken: " + endTimeInSeconds + " s (" + (endTimeInSeconds / 60.0) + " mins)");

            BufferedWriter out = new BufferedWriter(
                    new FileWriter(databaseName + ".log", true));
            Date date = new Date();
            out.write(date + " All " + numTransactions + " clients have completed their transactions.\n");
            out.write(date + " Number of transactions with error: " + numXactError + "\n");
            out.write(date + " Total time taken: " + endTimeInSeconds + " s (" + (endTimeInSeconds / 60.0) + " mins)\n");
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] readConfigFile() {
        String host = Constant.DEFAULT_HOST;
        String port = Constant.DEFAULT_PORT;
        String databaseName = Constant.DEFAULT_DATABASE;
        String consistencyLevel = Constant.DEFAULT_CONSISTENCY_LEVEL;
        String numberOfTransactions = Constant.DEFAULT_NUMBER_OF_TRANSACTIONS;

        String line;
        try {
            FileReader fr = new FileReader(Constant.CONFIGURATION_FILE);
            BufferedReader br = new BufferedReader(fr);

            while ((line = br.readLine()) != null) {
                String[] lineValue = line.split("=");
                if (lineValue[0].equals(Constant.HOST_KEY)) {
                    host = lineValue[1];
                } else if (lineValue[0].equals(Constant.PORT_KEY)) {
                    port = lineValue[1];
                } else if (lineValue[0].equals(Constant.DATABASE_KEY)) {
                    databaseName = lineValue[1];
                } else if (lineValue[0].equals(Constant.CONSISTENCY_LEVEL_KEY)) {
                    consistencyLevel = lineValue[1];
                } else if (lineValue[0].equals(Constant.NUMBER_OF_TRANSACTIONS_KEY)) {
                    numberOfTransactions = lineValue[1];
                }
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.out.println("Error reading " + Constant.CONFIGURATION_FILE);
            e.printStackTrace();
        }

        String[] configData = {host, port, databaseName, consistencyLevel, numberOfTransactions};
        return configData;
    }

    private static void outputPerformanceResults(Map<Integer, ClientStatistics> statisticsMap,
                                                 String consistencyLevel, int numTransactions) {
        int numClients = statisticsMap.size();
        String outputPath = Constant.getPerformanceOutputPath(consistencyLevel, numTransactions);

        try {
            BufferedWriter out = new BufferedWriter(
                    new FileWriter(outputPath, true));
            out.write("Consistency level: " + consistencyLevel + "\n");
            out.write("Number of clients: " + numTransactions + "\n\n");
            ClientStatistics min = statisticsMap.get(1);
            for (int i = 1; i <= numClients; i++) {
                min = statisticsMap.get(i);
                if (min != null)
                    break;
            }
            if (min == null) {
                out.write("No transactions found\n");
                out.close();
                return;
            }
            ClientStatistics max = min;

            double totalThroughput = 0;
            for (ClientStatistics current : statisticsMap.values()) {
                double currentThroughput = current.getThroughput();
                totalThroughput += currentThroughput;
                if (currentThroughput < min.getThroughput()) {
                    min = current;
                } else if (currentThroughput > max.getThroughput()) {
                    max = current;
                }
            }

            // Output statistics of each client:
            for (int i = 1; i <= numClients; i++) {
                ClientStatistics stats = statisticsMap.get(i);
                if (stats == null) {
                    out.write("Performance measure for client with index: " + i + "\n");
                    out.write("Data unavailable - Runtime error encountered.\n");
                    out.write("=========================================\n\n");
                    continue;
                }
                long totalTransactionCount = stats.getTotalTransactionCount();
                double totalExecutionTime = (double) stats.getTotalExecutionTime() / 1000000000;
                double throughput = stats.getThroughput();

                long[] transactionCountStats = stats.getAllTransactionCount();
                long[] executionTimeStats = stats.getAllExecutionTime();

                long newOrderTransactionCount = transactionCountStats[0];
                long paymentTransactionCount = transactionCountStats[1];
                long deliveryTransactionCount = transactionCountStats[2];
                long orderStatusTransactionCount = transactionCountStats[3];
                long stockLevelTransactionCount = transactionCountStats[4];
                long popularItemTransactionCount = transactionCountStats[5];
                long topBalanceTransactionCount = transactionCountStats[6];
                long relatedCustomerTransactionCount = transactionCountStats[7];

                double newOrderExecutionTime = (double) executionTimeStats[0] / 1000000000;
                double paymentExecutionTime = (double) executionTimeStats[1] / 1000000000;
                double deliveryExecutionTime = (double) executionTimeStats[2] / 1000000000;
                double orderStatusExecutionTime = (double) executionTimeStats[3] / 1000000000;
                double stockLevelExecutionTime = (double) executionTimeStats[4] / 1000000000;
                double popularItemExecutionTime = (double) executionTimeStats[5] / 1000000000;
                double topBalanceExecutionTime = (double) executionTimeStats[6] / 1000000000;
                double relatedCustomerExecutionTime = (double) executionTimeStats[7] / 1000000000;

                out.write("Performance measure for client with index: " + i + "\n");
                out.write("Total Number of Executed Transactions: " + totalTransactionCount + "\n");
                out.write("Total Execution Time: " + totalExecutionTime + "\n");
                out.write("Transaction throughput: " + throughput + "\n");
                out.write("-------------------------------------" + "\n");
                out.write("New Order: " + newOrderTransactionCount + " " + newOrderExecutionTime + "\n");
                out.write("Payment: " + paymentTransactionCount + " " + paymentExecutionTime + "\n");
                out.write("Delivery Status: " + deliveryTransactionCount + " " + deliveryExecutionTime + "\n");
                out.write("Order Status: " + orderStatusTransactionCount + " " + orderStatusExecutionTime + "\n");
                out.write("Stock Level: " + stockLevelTransactionCount + " " + stockLevelExecutionTime + "\n");
                out.write("Popular Item: " + popularItemTransactionCount + " " + popularItemExecutionTime + "\n");
                out.write("Top Balance: " + topBalanceTransactionCount + " " + topBalanceExecutionTime + "\n");
                out.write("Related Customer: " + relatedCustomerTransactionCount + " " + relatedCustomerExecutionTime + "\n");
                out.write("=========================================\n\n");
            }

            out.write("== End of Performance Measure for each Client ==\n\n");

            // Output minimum, maximum and average throughput
            out.write("Minimum transaction throughput is the client with index " + (min.getIndex())
                    + " with throughput: " + min.getThroughput() + "\n");
            out.write("Maximum transaction throughput is the client with index " + (max.getIndex())
                    + " with throughput: " + max.getThroughput() + "\n");
            out.write("Average transaction throughput is: " + (totalThroughput / numClients) + "\n\n");

            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error in outputting performance results.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}