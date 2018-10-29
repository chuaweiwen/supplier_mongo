package main.java;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OrderStatusTransaction {

    private MongoDatabase database;

    public OrderStatusTransaction(MongoDatabase database) {
        this.database = database;
    }

    public void processOrderStatus(int wId, int dId, int cId) {
        Document customer = selectCustomer(wId, dId, cId);

        String firstName = customer.getString(Customer.C_FIRST);
        String middleName = customer.getString(Customer.C_MIDDLE);
        String lastName = customer.getString(Customer.C_LAST);
        double balance = customer.getDouble(Customer.C_BALANCE);

        int oId = customer.getInteger(Customer.C_LAST_O_ID);
        String entryDate = customer.getString(Customer.C_LAST_O_ENTRY_D);
        int carrierId = 0;
        boolean isNull = false;
        try {
            carrierId = customer.getInteger(Customer.C_LAST_O_CARRIER_ID);
        } catch (ClassCastException e) {
            isNull = true;
        }

        Document order = selectOrder(wId, dId, cId, oId);
        List<Document> orderLines = (List<Document>) order.get(Order.O_ORDERLINES);

        // Print customer information
        System.out.println("Customer's name: " + firstName + " " + middleName + " " + lastName);
        System.out.println("Customer's balance: " + balance);

        // Print customer's last order
        System.out.println("Last Order ID: " + oId);
        System.out.println("Entry date and time: " + entryDate);
        if (isNull) {
            System.out.println("Carrier identifier: null");
        } else {
            System.out.println("Carrier identifier: " + carrierId);
        }

        System.out.println("====================");

        // Print each last order lines
        for (Document orderLine : orderLines) {
            int iId = orderLine.getInteger(OrderLine.OL_I_ID);
            int supplierWId = orderLine.getInteger(OrderLine.OL_SUPPLY_W_ID);
            int quantity = orderLine.getInteger(OrderLine.OL_QUANTITY);
            double amount = orderLine.getDouble(OrderLine.OL_AMOUNT);
            String deliveryData = orderLine.getString(OrderLine.OL_DELIVERY_D);

            System.out.println("Item number: " + iId);
            System.out.println("Supplying warehouse number: " + supplierWId);
            System.out.println("Quantity ordered " + quantity);
            System.out.println("Total price for ordered item: " + amount);
            System.out.println("Data and time of delivery: " + deliveryData);
            System.out.println();
        }
    }

    private Document selectCustomer(int wId, int dId, int cId) {
        MongoCollection<Document> collection = database.getCollection(Table.CUSTOMER);
        return collection.find(and(
                eq(Customer.C_W_ID, wId),
                eq(Customer.C_D_ID, dId),
                eq(Customer.C_ID, cId)
        )).first();
    }

    private Document selectOrder(int wId, int dId, int cId, int oId) {
        MongoCollection<Document> collection = database.getCollection(Table.ORDER);
        return collection.find(and(
                eq(Order.O_W_ID, wId),
                eq(Order.O_D_ID, dId),
                eq(Order.O_C_ID, cId),
                eq(Order.O_ID, oId)
        )).first();
    }
}
