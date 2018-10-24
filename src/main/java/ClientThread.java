package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.Callable;

import main.java.transaction.Transaction;

public class ClientThread implements Callable<ClientStatistics> {
    public static final char XACT_NEW_ORDER = 'N';
    public static final char XACT_PAYMENT = 'P';
    public static final char XACT_DELIVERY = 'D';
    public static final char XACT_ORDER_STATUS = 'O';
    public static final char XACT_STOCK_LEVEL = 'S';
    public static final char XACT_POPULAR_ITEM = 'I';
    public static final char XACT_TOP_BALANCE = 'T';
    public static final char XACT_RELATED_CUSTOMER = 'R';

    private int index;
    private String consistencyLevel;
    private Transaction transaction;
    private String host;
    private int port;
    private String database;

    public ClientThread(int index, String consistencyLevel, String host, int port, String database) {
        this.index = index;
        this.consistencyLevel = consistencyLevel;
        this.host = host;
        this.port = port;
        this.database = database;
    }

    public ClientStatistics readTransaction() {
        File file = new File(Constant.getTransactionFileLocation(index+1));
        transaction = new Transaction(index, this.consistencyLevel, this.host, this.port, this.database);
        long[] transactionCount = new long[8];
        long[] executionTime = new long[8];

        long startTime;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String input = reader.readLine();

            while (input != null && input.length() > 0) {
                String[] arguments = input.split(",");

                if (input.charAt(0) == XACT_NEW_ORDER) {
                    int cId = Integer.parseInt(arguments[1]);
                    int wId = Integer.parseInt(arguments[2]);
                    int dId = Integer.parseInt(arguments[3]);
                    int numItems = Integer.parseInt(arguments[4]);
                    int[] itemNumbers = new int[numItems];
                    int[] supplierWarehouse = new int[numItems];
                    int[] quantity = new int[numItems];

                    String newInputLine;
                    String[] newArguments;
                    for (int j = 0; j < numItems; j++) {
                        newInputLine = reader.readLine();
                        newArguments = newInputLine.split(",");

                        itemNumbers[j] = Integer.parseInt(newArguments[0]);
                        supplierWarehouse[j] = Integer.parseInt(newArguments[1]);
                        quantity[j] = Integer.parseInt(newArguments[2]);
                    }

                    try {
                        startTime = System.nanoTime();
                        transaction.processNewOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(0, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_PAYMENT) {
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int cId = Integer.parseInt(arguments[3]);
                    float payment = Float.parseFloat(arguments[4]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processPayment(wId, dId, cId, payment);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(1, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_DELIVERY) {
                    int wId = Integer.parseInt(arguments[1]);
                    int carrierId = Integer.parseInt(arguments[2]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processDelivery(wId, carrierId);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(2, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_ORDER_STATUS) { // Order Status
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int cId = Integer.parseInt(arguments[3]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processOrderStatus(wId, dId, cId);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(3, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_STOCK_LEVEL) { // Stock Level
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int T = Integer.parseInt(arguments[3]);
                    int L = Integer.parseInt(arguments[4]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processStockLevel(wId, dId, (long) T, L);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(4, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_POPULAR_ITEM) { // Popular item
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int L = Integer.parseInt(arguments[3]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processPopularItem(wId, dId, L);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(5, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_TOP_BALANCE) { // Top-Balance
                    try {
                        startTime = System.nanoTime();
                        transaction.processTopBalance();
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(6, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_RELATED_CUSTOMER) {
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int cId = Integer.parseInt(arguments[3]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processRelatedCustomer(wId, dId, cId);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(7, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else {
                    System.err.println("\n\nOops, the application encountered an error in reading file.\n\n");
                }
                System.out.println();
                input = reader.readLine();
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ClientStatistics(index, transactionCount, executionTime);
    }

    private void updateTransactionDetail(int index, long endTime, long[] executionTime,
                                         long[] transactionCount) {
        executionTime[index] = executionTime[index] + endTime;
        transactionCount[index] = transactionCount[index]++;
    }

    @Override
    public ClientStatistics call() {
        return readTransaction();
    }
}
