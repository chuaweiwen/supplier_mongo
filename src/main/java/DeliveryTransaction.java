package main.java;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


class DeliveryTransaction {
    private MongoDatabase database;

    DeliveryTransaction(MongoDatabase database) {
        this.database = database;
    }

    /*
     *  1. For DISTRICT NO = 1 to 10
     *   (a) Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
     *       with O CARRIER ID = null; i.e.,
     *       N = min{t.O ID ∈ Order | t.O W ID = W ID, t.D ID = DISTRICT NO, t.O CARRIER ID = null}
     *       Let X denote the order corresponding to order number N, and let C denote the customer
     *       who placed this order
     *   (b) Update the order X by setting O CARRIER ID to CARRIER ID
     *   (c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
     *   (d) Update customer C as follows:
     *      • Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the
     *        items placed in order X
     *      • Increment C DELIVERY CNT by 1
     */


    void processDelivery(int W_ID, int CARRIER_ID) {
        for (int D_ID = 1; D_ID <= 10; D_ID++) {
            DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String currentDate = DF.format(new Date());
            Document order = selectOldestOrder(W_ID, D_ID);
            if (order == null) {
                continue;
            }

            updateOrderCarrierId(order, CARRIER_ID);
            double sumOlAmount = updateOrderLines(order, currentDate);
            updateCustomer(order, CARRIER_ID, sumOlAmount);
        }
    }

    /**
     * Increment C_BALANCE by sum of OL_AMOUNT for all the items placed in order
     * Increment C_DELIVERY_CNT by 1
     */
    private void updateCustomer(Document order, int CARRIER_ID, double totalAmount) {
        MongoCollection<Document> collection = database.getCollection(Table.CUSTOMER);

        Document find = new Document();
        find.put(Customer.C_W_ID, order.getInteger(Order.O_W_ID));
        find.put(Customer.C_D_ID, order.getInteger(Order.O_D_ID));
        find.put(Customer.C_ID, order.getInteger(Order.O_C_ID));

        Document customer = collection.find(find).first();
        //System.out.println("initial cust: " + customer);

        Document carrier = new Document();
        Document set = new Document("$set", carrier);
        carrier.put(Customer.C_BALANCE, customer.getDouble(Customer.C_BALANCE) + totalAmount);
        carrier.put(Customer.C_DELIVERY_CNT, customer.getInteger(Customer.C_DELIVERY_CNT) + 1);

        int C_LAST_O_ID = customer.getInteger(Customer.C_LAST_O_ID);
        if (C_LAST_O_ID == order.getInteger(Order.O_ID)) {
            carrier.put(Customer.C_LAST_O_CARRIER_ID, CARRIER_ID);
            //System.out.println("Update C_LAST_O_CARRIER_ID : " + C_LAST_O_ID + " " + CARRIER_ID);
        }

        collection.updateOne(find, set);
        //System.out.println("Updated cust: " + collection.find(find).first());
    }

    /**
     * Update all the order-lines in order by setting OL_DELIVERY_D to the current date and time
     */
    private double updateOrderLines(Document order, String OL_DELIVERY_D) {
        MongoCollection<Document> orderOrderlineCollection = database.getCollection(Table.ORDER);

        // List of Document embedded in the order Document
        List<Document> targetOrderlines = (List<Document>) order.get(Order.O_ORDERLINES);

        double totalAmount = 0;

        for (Document ol : targetOrderlines) {
            totalAmount += ol.getDouble(OrderLine.OL_AMOUNT);

            //db.orders.update({ o_w_id: 1, o_d_id: 1, o_id: 2107, "o_orderlines.ol_number": 1}, {"$set": {"o_orderlines.$.ol_delivery_d": "testinggg"}} )
            Document targetedOrderline = new Document();
            targetedOrderline.put(Order.O_W_ID, order.getInteger(Order.O_W_ID));
            targetedOrderline.put(Order.O_D_ID, order.getInteger(Order.O_D_ID));
            targetedOrderline.put(Order.O_ID, order.getInteger(Order.O_ID));
            targetedOrderline.put(Order.O_ORDERLINES + "." + OrderLine.OL_NUMBER, ol.getInteger(OrderLine.OL_NUMBER));

            Document carrier = new Document();
            Document set = new Document("$set", carrier);
            carrier.put(Order.O_ORDERLINES + ".$." + OrderLine.OL_DELIVERY_D, OL_DELIVERY_D);

            orderOrderlineCollection.updateOne(targetedOrderline, set);
        }

        //System.out.println("Sum of Order lines amount: " + totalAmount);
        return totalAmount;
    }

    private void updateOrderCarrierId(Document order, int CARRIER_ID) {
        MongoCollection<Document> orderOrderlinecollection = database.getCollection(Table.ORDER);

        Document carrier = new Document();
        Document set = new Document("$set", carrier);
        carrier.put(Order.O_CARRIER_ID, CARRIER_ID);

        orderOrderlinecollection.updateOne(order, set);
    }

    /**
     * Find the smallest order number O_ID for district(W_ID, DISTRICT_NO) with O_CARRIER_ID = null
     */
    private Document selectOldestOrder(int W_ID, int D_ID) {
        MongoCollection<Document> orderOrderlinecollection = database.getCollection(Table.ORDER);

        Document searchOldestOrderQuery = new Document();
        searchOldestOrderQuery.append(Order.O_W_ID, W_ID);
        searchOldestOrderQuery.append(Order.O_D_ID, D_ID);
        searchOldestOrderQuery.append(Order.O_CARRIER_ID, "null");

        Document sortQuery = new Document();
        sortQuery.append(Order.O_ID, 1);

        FindIterable<Document> orders = orderOrderlinecollection.find(searchOldestOrderQuery).sort(sortQuery).limit(1);
        Document result = orders.first();

        //System.out.println("Oldest order (with null carrier id) is: " + result.getInteger(Order.O_ID));
        return result;
    }
}
