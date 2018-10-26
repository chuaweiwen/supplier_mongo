package main.java;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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

        ExecutorService executorService = Executors.newFixedThreadPool(Math.max(1, numTransactions));
        List<Future<ClientStatistics>> results = new ArrayList<Future<ClientStatistics>>();

        for (int i = 1; i <= numTransactions; i++) {
            Future<ClientStatistics> future =
                    executorService.submit(new ClientThread(i, consistencyLevel, host, port, databaseName));
            results.add(future);
        }

        Map<Integer, ClientStatistics> statisticsMap = new HashMap<Integer, ClientStatistics>();
        for (Future<ClientStatistics> future : results) {
            try {
                ClientStatistics statistics = future.get();
                statisticsMap.put(statistics.getIndex(), statistics);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return;
            }
        }

        outputPerformanceResults(statisticsMap);
        System.out.println("\nAll " + numTransactions + " clients have completed their transactions.");
    }

    private static String[] readConfigFile() {
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

    private static void outputPerformanceResults(Map<Integer, ClientStatistics> statisticsMap) {
        int numClients = statisticsMap.size();
        String outputPath = Constant.PERFORMANCE_OUTPUT_PATH;

        try {
            PrintWriter out = new PrintWriter(outputPath);
            ClientStatistics min = statisticsMap.get(0);
            ClientStatistics max = statisticsMap.get(0);
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
            for (int i = 0; i < numClients; i++) {
                ClientStatistics stats = statisticsMap.get(i);
                int index = i + 1;
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

                out.println("Performance measure for client with index: " + index);
                out.println("Total Number of Executed Transactions: " + totalTransactionCount);
                out.println("Total Execution Time: " + totalExecutionTime);
                out.println("Transaction throughput: " + throughput);
                out.println("-------------------------------------");
                out.println("New Order: " + newOrderTransactionCount + " " + newOrderExecutionTime);
                out.println("Payment: " + paymentTransactionCount + " " + paymentExecutionTime);
                out.println("Delivery Status: " + deliveryTransactionCount + " " + deliveryExecutionTime);
                out.println("Order Status: " + orderStatusTransactionCount + " " + orderStatusExecutionTime);
                out.println("Stock Level: " + stockLevelTransactionCount + " " + stockLevelExecutionTime);
                out.println("Popular Item: " + popularItemTransactionCount + " " + popularItemExecutionTime);
                out.println("Top Balance: " + topBalanceTransactionCount + " " + topBalanceExecutionTime);
                out.println("Related Customer: " + relatedCustomerTransactionCount + " " + relatedCustomerExecutionTime);
                out.println("=========================================");
                out.println();
            }

            out.println("== End of Performance Measure for each Client ==");
            out.println();

            // Output minimum, maximum and average throughput
            out.println("Minimum transaction throughput is the client with index " + (min.getIndex() + 1)
                    + " with throughput: " + min.getThroughput());
            out.println("Minimum transaction throughput is the client with index " + (min.getIndex() + 1)
                    + " with throughput: " + min.getThroughput());
            out.println("Average transaction throughput is: " + (totalThroughput / numClients));

            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error in outputting performance results.");
            e.printStackTrace();
        }
    }
}