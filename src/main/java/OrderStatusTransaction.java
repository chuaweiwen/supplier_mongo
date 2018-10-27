package main.java;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.Date;
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
        Date entryDate = customer.getDate(Customer.C_LAST_O_ENTRY_D);
        int carrierId = customer.getInteger(Customer.C_LAST_O_CARRIER_ID);

        Document order = selectOrder(oId);
        List<Document> orderLines = (List<Document>) order.get(Order.O_ORDERLINES);

        // Print customer information
        System.out.println("Customer's name: " + firstName + " " + middleName + " " + lastName);
        System.out.println("Customer's balance: " + balance);

        // Print customer's last order
        System.out.println("Last Order ID: " + oId);
        System.out.println("Entry date and time: " + entryDate);
        System.out.println("Carrier identifier: " + carrierId);

        System.out.println("====================");

        // Print each last order lines
        for (Document orderLine : orderLines) {
            int iId = orderLine.getInteger(OrderLine.OL_I_ID);
            int supplierWId = orderLine.getInteger(OrderLine.OL_SUPPLY_W_ID);
            double quantity = orderLine.getDouble(OrderLine.OL_QUANTITY);
            double amount = orderLine.getDouble(OrderLine.OL_AMOUNT);
            Date deliveryData = orderLine.getDate(OrderLine.OL_DELIVERY_D);

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

    private Document selectOrder(int oId) {
        MongoCollection<Document> collection = database.getCollection(Table.ORDER_ORDERLINE);
        return collection.find(eq(Order.O_ID, oId)).first();
    }
}
