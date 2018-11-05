package main.java;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NewOrderTransaction {

    private MongoDatabase database;

    private static final String CLASS_NAME = "NewOrderTransaction";
    private static final String EXCEPTION_DISTRICTS_NOT_FOUND = CLASS_NAME + " - Districts not found";
    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public NewOrderTransaction(MongoDatabase database) {
        this.database = database;
    }

    public void processOrder(int wId, int dId, int cId, int numItems,
                             int[] itemNum, int[] supplierWarehouse, int[] qty) {
        Document warehouse = selectWarehouse(wId);
        Document customer = selectCustomer(wId, dId, cId);

        List<Document> districts = (List<Document>) warehouse.get(Warehouse.W_DISTRICTS);

        Document district = null;
        for (Document doc : districts) {
            if (doc.getInteger(District.D_ID) == dId) {
                district = doc;
                break;
            }
        }

        if (district == null) {
            System.out.println(EXCEPTION_DISTRICTS_NOT_FOUND);
            return;
        }

        double wTax = warehouse.getDouble(Warehouse.W_TAX);
        double dTax = district.getDouble(District.D_TAX);
        int nextOId = district.getInteger(District.D_NEXT_O_ID);
        updateDistrictNextOId(nextOId + 1, wId, dId);

        String currentDate = DF.format(new Date());

        double allLocal = 1;
        String[] itemOutput = new String[numItems];
        for (int i = 0; i < numItems; i++) {
            if (supplierWarehouse[i] != wId) {
                allLocal = 0;
                break;
            }
        }

        double totalAmount= 0;
        List<Document> orderLines = new ArrayList<Document>();
        for (int i = 0; i < numItems; i++) {
            int iId = itemNum[i];
            int iWId = supplierWarehouse[i];
            int quantity = qty[i];

            Document stock = selectStock(iWId, iId);
            double adjQuantity = 0.0;
            try {
                adjQuantity = stock.getDouble(Stock.S_QUANTITY) - quantity;
            } catch (ClassCastException e) {
                adjQuantity = (double) stock.getInteger(Stock.S_QUANTITY) - quantity;
            }
            while (adjQuantity < 10) {
                adjQuantity += 100;
            }

            updateStock(iWId, iId, adjQuantity,
                    stock.getDouble(Stock.S_YTD) + quantity,
                    stock.getInteger(Stock.S_ORDER_CNT) + 1,
                    (iWId != wId)
                            ? stock.getInteger(Stock.S_REMOTE_CNT) + 1
                            : stock.getInteger(Stock.S_REMOTE_CNT));

            String itemName = stock.getString(Stock.S_I_NAME);
            double itemAmount = stock.getDouble(Stock.S_I_PRICE) * quantity;
            totalAmount += itemAmount;

            // Order line
            orderLines.add(new Document(OrderLine.OL_W_ID, wId)
                    .append(OrderLine.OL_D_ID, dId)
                    .append(OrderLine.OL_O_ID, nextOId)
                    .append(OrderLine.OL_NUMBER, i)
                    .append(OrderLine.OL_I_ID, iId)
                    .append(OrderLine.OL_I_NAME, itemName)
                    .append(OrderLine.OL_AMOUNT, itemAmount)
                    .append(OrderLine.OL_SUPPLY_W_ID, iWId)
                    .append(OrderLine.OL_QUANTITY, quantity)
                    .append(OrderLine.OL_DIST_INFO, stock.getString(Stock.getDistrictStringId(dId))));

            itemOutput[i] = "" + (i+1) + "\t" + itemName + "\t" + iWId + " " + quantity +
                    "\t" + itemAmount + "\t" + adjQuantity;
        }
        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getDouble(Customer.C_DISCOUNT));

        database.getCollection(Table.ORDER).insertOne(
                new Document(Order.O_W_ID, wId)
                        .append(Order.O_D_ID, dId)
                        .append(Order.O_ENTRY_D, currentDate)
                        .append(Order.O_ID, nextOId)
                        .append(Order.O_C_ID, cId)
                        .append(Order.O_OL_CNT, numItems)
                        .append(Order.O_ALL_LOCAL, allLocal)
                        .append(Order.O_C_FIRST, customer.getString(Customer.C_FIRST))
                        .append(Order.O_C_MIDDLE, customer.getString(Customer.C_MIDDLE))
                        .append(Order.O_C_LAST, customer.getString(Customer.C_LAST))
                        .append(Order.O_ORDERLINES, orderLines));
        updateCustomerLastOrder(nextOId, wId, dId, cId, currentDate);
        
        /**
         * Outputs the necessary data
         */
        System.out.println("Customer (" + wId + ", " + dId + ", " + cId + ")"
                + " C_LAST: " + customer.getString(Customer.C_LAST)
                + " C_CREDIT: " + customer.getString(Customer.C_CREDIT)
                + " C_DISCOUNT: " + customer.getDouble(Customer.C_DISCOUNT));
        System.out.println("Warehouse tax: " + wTax + ", district tax: " + dTax);
        System.out.println("Order number: " + nextOId + ", entry date: " + currentDate);
        System.out.println("Number of items: " + numItems + ", total amount: " + totalAmount);
        for (String s : itemOutput) {
            System.out.println(s);
        }
    }

    private Document selectWarehouse(int wId) {
        MongoCollection<Document> collection = database.getCollection(Table.WAREHOUSE);
        return collection.find(eq(Warehouse.W_ID, wId)).first();
    }

    private Document selectCustomer(int wId, int dId, int cId) {
        MongoCollection<Document> collection = database.getCollection(Table.CUSTOMER);
        return collection.find(and(
                eq(Customer.C_W_ID, wId),
                eq(Customer.C_D_ID, dId),
                eq(Customer.C_ID, cId)
        )).first();
    }

    private Document selectStock(int iWId, int iId) {
        MongoCollection<Document> collection = database.getCollection(Table.STOCK);
        return collection.find(and(
                eq(Stock.S_W_ID, iWId),
                eq(Stock.S_I_ID, iId)
        )).first();
    }

    private void updateDistrictNextOId(int nextOId, int wId, int dId) {
        MongoCollection collection = database.getCollection(Table.WAREHOUSE);

        Document searchQuery = new Document();
        searchQuery.put(Warehouse.W_ID, wId);
        searchQuery.put(Warehouse.W_DISTRICTS + "." + District.D_ID, dId);

        Document newDocument = new Document();
        Document updateQuery = new Document("$set", newDocument);
        newDocument.put(Warehouse.W_DISTRICTS + ".$." + District.D_NEXT_O_ID, nextOId);

        collection.updateOne(searchQuery, updateQuery);
    }

    private void updateCustomerLastOrder(int nextOId, int wId, int dId, int cId, String currentDate) {
        MongoCollection<Document> collection = database.getCollection(Table.CUSTOMER);

        Document searchQuery = new Document();
        searchQuery.put(Customer.C_W_ID, wId);
        searchQuery.put(Customer.C_D_ID, dId);
        searchQuery.put(Customer.C_ID, cId);

        Document newDocument = new Document();
        Document updateQuery = new Document("$set", newDocument);
        newDocument.put(Customer.C_LAST_O_ID, nextOId);
        newDocument.put(Customer.C_LAST_O_ENTRY_D, currentDate);
        newDocument.put(Customer.C_LAST_O_CARRIER_ID, "null");

        collection.updateOne(searchQuery, updateQuery);
    }

    private void updateStock(int wId, int iId, double qty, double sYtd, int orderCnt, int remoteCnt) {
        MongoCollection<Document> collection = database.getCollection(Table.STOCK);

        Document find = new Document();
        find.put(Stock.S_W_ID, wId);
        find.put(Stock.S_I_ID, iId);

        Document carrier = new Document();
        Document set = new Document("$set", carrier);
        carrier.put(Stock.S_QUANTITY, qty);
        carrier.put(Stock.S_YTD, sYtd);
        carrier.put(Stock.S_ORDER_CNT, orderCnt);
        carrier.put(Stock.S_REMOTE_CNT, remoteCnt);

        collection.updateOne(find, set);
    }
}
