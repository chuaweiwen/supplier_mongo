package main.java.transaction;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

public class Transaction {
    private NewOrderTransaction newOrderTransaction;
    private PaymentTransaction paymentTransaction;
    private DeliveryTransaction deliveryTransaction;
    private OrderStatusTransaction orderStatusTransaction;
    private StockLevelTransaction stockLevelTransaction;
    private PopularItemTransaction popularItemTransaction;
    private TopBalanceTransaction topBalanceTransaction;
    private RelatedCustomerTransaction relatedCustomerTransaction;

    public Transaction(int index, String consistencyLevel, String host, int port, String databaseName) {
        MongoClient mongoClient;
        if (consistencyLevel.equalsIgnoreCase("local")) {
            mongoClient = new MongoClient(
                    new ServerAddress(host, port),
                    new MongoClientOptions.Builder()
                            .writeConcern(WriteConcern.W1)
                            .readConcern(ReadConcern.LOCAL)
                            .build());
        } else {
            mongoClient = new MongoClient(
                    new ServerAddress(host, port),
                    new MongoClientOptions.Builder()
                            .writeConcern(WriteConcern.MAJORITY)
                            .readConcern(ReadConcern.MAJORITY)
                            .build());
        }
        MongoDatabase database = mongoClient.getDatabase(databaseName);

        newOrderTransaction = new NewOrderTransaction(database);
        //paymentTransaction = new PaymentTransaction(database);
        //deliveryTransaction = new DeliveryTransaction(database);
        //orderStatusTransaction = new OrderStatusTransaction(database);
        //stockLevelTransaction = new StockLevelTransaction(database);
        //popularItemTransaction = new PopularItemTransaction(database);
        //topBalanceTransaction = new TopBalanceTransaction(database);
        //relatedCustomerTransaction = new RelatedCustomerTransaction(database);
    }

    public void processNewOrder(int wId, int dId, int cId, int numItems,
                                int[] itemNum, int[] supplierWarehouse, int[] qty) {
        newOrderTransaction.processOrder(wId, dId, cId, numItems, itemNum, supplierWarehouse, qty);
    }

    public void processPayment(int wId, int dId, int cId, double payment) {
        //paymentTransaction.processPaymentTransaction(wId, dId, cId, payment);
    }

    public void processDelivery(int wId, int carrierId) {
        //deliveryTransaction.processDeliveryTransaction(wId, carrierId);
    }

    public void processOrderStatus(int wId, int dId, int cId) {
        //orderStatusTransaction.processOrderStatus(wId, dId, cId);
    }

    public void processStockLevel(int wId, int dId, long T, int L) {
        //stockLevelTransaction.processStockLevelTransaction(wId, dId, T, L);
    }

    public void processPopularItem(int wId, int dId, int L) {
        //popularItemTransaction.popularItem(wId, dId, L);
    }

    public void processTopBalance() {
        //topBalanceTransaction.calTopBalance();
    }

    public void processRelatedCustomer(int wId, int dId, int cId) {
        //relatedCustomerTransaction.relatedCustomer(wId, dId, cId);
    }
}
