package main.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Date;
import java.util.concurrent.Callable;

public class ClientThread implements Callable<ClientStatistics> {
    private static final char XACT_NEW_ORDER = 'N';
    private static final char XACT_PAYMENT = 'P';
    private static final char XACT_DELIVERY = 'D';
    private static final char XACT_ORDER_STATUS = 'O';
    private static final char XACT_STOCK_LEVEL = 'S';
    private static final char XACT_POPULAR_ITEM = 'I';
    private static final char XACT_TOP_BALANCE = 'T';
    private static final char XACT_RELATED_CUSTOMER = 'R';

    private int index;
    private String consistencyLevel;
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

    private ClientStatistics readTransaction() {
        File file = new File(Constant.getTransactionFileLocation(index));
        Transaction transaction = new Transaction(this.consistencyLevel, this.host, this.port, this.database);
        long[] transactionCount = new long[8];
        long[] executionTime = new long[8];
        int i = 0;

        long startTime;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String input = reader.readLine();

            while (input != null && input.length() > 0) {
                String[] arguments = input.split(",");
                i++;

                if (input.charAt(0) == XACT_NEW_ORDER) {
                    //System.out.println(index + " " + XACT_NEW_ORDER + " " + i);
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
                    //System.out.println(index + " " + XACT_PAYMENT + " " + i);
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
                    //System.out.println(index + " " + XACT_DELIVERY + " " + i);
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
                    //System.out.println(index + " " + XACT_ORDER_STATUS + " " + i);
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
                    //System.out.println(index + " " + XACT_STOCK_LEVEL + " " + i);
                    int wId = Integer.parseInt(arguments[1]);
                    int dId = Integer.parseInt(arguments[2]);
                    int T = Integer.parseInt(arguments[3]);
                    int L = Integer.parseInt(arguments[4]);

                    try {
                        startTime = System.nanoTime();
                        transaction.processStockLevel(wId, dId, T, L);
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(4, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_POPULAR_ITEM) { // Popular item
                    //System.out.println(index + " " + XACT_POPULAR_ITEM + " " + i);
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
                    //System.out.println(index + " " + XACT_TOP_BALANCE + " " + i);
                    try {
                        startTime = System.nanoTime();
                        transaction.processTopBalance();
                        long endTime = System.nanoTime() - startTime;
                        updateTransactionDetail(6, endTime, executionTime, transactionCount);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (input.charAt(0) == XACT_RELATED_CUSTOMER) {
                    //System.out.println(index + " " + XACT_RELATED_CUSTOMER + " " + i);
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

                if (i % 1000 == 0) {
                    BufferedWriter out = new BufferedWriter(
                            new FileWriter(database + ".log", true));
                    Date date = new Date();
                    out.write(date + " Client " + index + " has executed " + i + " transactions.\n");
                    out.close();
                }
                input = reader.readLine();
            }

            BufferedWriter out = new BufferedWriter(
                    new FileWriter(database + ".log", true));
            Date date = new Date();
            out.write(date + " Client " + index + " has executed all its transactions.\n");
            out.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ClientStatistics(index, transactionCount, executionTime);
    }

    private void updateTransactionDetail(int index, long endTime, long[] executionTime,
                                         long[] transactionCount) {
        executionTime[index] = executionTime[index] + endTime;
        transactionCount[index]++;
    }

    @Override
    public ClientStatistics call() {
        return readTransaction();
    }
}
