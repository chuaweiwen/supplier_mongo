package main.java;

import java.util.concurrent.Callable;

public class ClientThread implements Callable<Triple<Integer, Long, Double>> {
    public static final String XACT_NEW_ORDER = "N";
    public static final String XACT_PAYMENT = "P";
    public static final String XACT_DELIVERY = "D";
    public static final String XACT_ORDER_STATUS = "O";
    public static final String XACT_STOCK_LEVEL = "S";
    public static final String XACT_POPULAR_ITEM = "I";
    public static final String XACT_TOP_BALANCE = "T";
    public static final String XACT_RELATED_CUSTOMER = "R";

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

    @Override
    public Triple<Integer, Long, Double> call() {
        return null;
    }
}
