package main.java.transaction;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import main.java.Constant;

public class NewOrderTransaction {

    private MongoDatabase database;

    public NewOrderTransaction(MongoDatabase database) {
        this.database = database;
    }

    public void processOrder(int wId, int dId, int cId, int numItems,
                             int[] itemNum, int[] supplierWarehouse, int[] qty) {
        Document warehouse = selectWarehouse(wId);
        Document customer = selectCustomer(wId, dId, cId);

        List<Document> districts = (List<Document>) warehouse.get("w_districts");

        Document district = null;
        for (Document doc : districts) {
            if (doc.getInteger("d_id") == dId) {
                district = doc;
                break;
            }
        }

        double wTax = warehouse.getDouble("w_tax");
        double dTax = district.getDouble("d_tax");
        int nextOId = district.getInteger("d_next_o_id");
        updateDistrictNextOId(nextOId + 1, wId, dId);

        Date currentDate = new Date();

        double allLocal = 1;
        String[] itemOutput = new String[numItems];
        for (int i = 0; i < numItems; i++) {
            if (supplierWarehouse[i] != wId) {
                allLocal = 0;
                break;
            }
        }
        updateCustomerLastOrder(nextOId, wId, dId, cId, currentDate);

        double totalAmount= 0;
        List<Document> orderLines = new ArrayList<Document>();
        for (int i = 0; i < numItems; i++) {
            int iId = itemNum[i];
            int iWId = supplierWarehouse[i];
            double quantity = qty[i];

            Document stock = selectStock(iWId, iId);
            double adjQuantity = stock.getDouble("s_quantity") - quantity;
            while (adjQuantity < 10) {
                adjQuantity += 100;
            }

            updateStock(iWId, iId, adjQuantity,
                    stock.getDouble("s_ytd") + quantity,
                    stock.getInteger("s_order_cnt") + 1,
                    (iWId != wId)
                            ? stock.getInteger("s_remote_cnt") + 1
                            : stock.getInteger("s_remote_cnt"));

            Document item = selectItem(iId);
            String itemName = item.getString("i_name");
            double itemAmount = item.getDouble("i_price") * quantity;
            totalAmount += itemAmount;

            // Order line
            orderLines.add(new Document("ol_w_id", wId)
                    .append("ol_d_id", dId)
                    .append("ol_o_id", nextOId)
                    .append("ol_number", i)
                    .append("ol_i_id", iId)
                    .append("ol_i_name", itemName)
                    .append("ol_amount", itemAmount)
                    .append("ol_supply_w_id", iWId)
                    .append("ol_quantity", quantity)
                    .append("ol_dist_info", stock.getString(getDistrictStringId(dId))));
        }
        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getDouble("c_discount"));

        database.getCollection("orders").insertOne(
                new Document("o_w_id", wId)
                        .append("o_d_id", dId)
                        .append("o_entry_d", currentDate)
                        .append("o_id", nextOId)
                        .append("o_c_id", cId)
                        .append("o_ol_cnt", numItems)
                        .append("o_all_local", allLocal)
                        .append("o_c_first", customer.getString("c_first"))
                        .append("o_c_middle", customer.getString("c_middle"))
                        .append("o_c_last", customer.getString("c_last"))
                        .append("o_ols", orderLines));

        /**
         * Outputs the necessary data
         */
        System.out.println("Customer (" + wId + ", " + dId + ", " + cId + ")"
                + " C_LAST: " + customer.getString("c_last")
                + " C_CREDIT: " + customer.getString("c_credit")
                + " C_DISCOUNT: " + customer.getDouble("c_discount"));
        System.out.println("Warehouse tax: " + wTax + ", district tax: " + dTax);
        System.out.println("Order number: " + nextOId + ", entry date: " + currentDate);
        System.out.println("Number of items: " + numItems + ", total amount: " + totalAmount);
        for (String s : itemOutput) {
            System.out.println(s);
        }
    }

    private Document selectWarehouse(int wId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_WAREHOUSE_DISTRICT);
        return collection.find(eq("w_id", wId)).first();
    }

    private Document selectCustomer(int wId, int dId, int cId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_CUSTOMER);
        return collection.find(and(
                eq("c_w_id", wId),
                eq("c_d_id", dId),
                eq("c_id", cId)
        )).first();
    }

    private Document selectItem(int iId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_ITEM);
        return collection.find(eq("i_id", iId)).first();
    }

    private Document selectStock(int iWId, int iId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_STOCK);
        return collection.find(and(
                eq("s_w_id", iWId),
                eq("s_i_id", iId)
        )).first();
    }

    private void updateDistrictNextOId(int nextOId, int wId, int dId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_WAREHOUSE_DISTRICT);

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("w_id", wId);
        searchQuery.put("w_districts.d_id", dId);

        BasicDBObject newDocument = new BasicDBObject();
        BasicDBObject updateQuery = new BasicDBObject("$set", newDocument);
        newDocument.put("w_districts.$.d_next_o_id", nextOId);

        collection.updateOne(searchQuery, updateQuery);
    }

    private void updateCustomerLastOrder(int nextOId, int wId, int dId, int cId, Date currentDate) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_CUSTOMER);

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("c_w_id", wId);
        searchQuery.put("c_d_id", dId);
        searchQuery.put("c_id", cId);

        BasicDBObject newDocument = new BasicDBObject();
        BasicDBObject updateQuery = new BasicDBObject("$set", newDocument);
        newDocument.put("c_last_order", nextOId);
        newDocument.put("c_entry_d", currentDate);

        collection.updateOne(searchQuery, updateQuery);
    }

    private void updateStock(int wId, int iId, double qty, double sYtd, int orderCnt, int remoteCnt) {
        MongoCollection<Document> collection = database.getCollection("stocks");

        BasicDBObject find = new BasicDBObject();
        find.put("s_w_id", wId);
        find.put("s_i_id", iId);

        BasicDBObject carrier = new BasicDBObject();
        BasicDBObject set = new BasicDBObject("$set", carrier);
        carrier.put("s_quantity", qty);
        carrier.put("s_ytd", sYtd);
        carrier.put("s_order_cnt", orderCnt);
        carrier.put("s_remote_cnt", remoteCnt);

        collection.updateOne(find, set);
    }

    private String getDistrictStringId(int dId) {
        if (dId < 10) {
            return "S_DIST_0" + dId;
        } else {
            return "S_DIST_10";
        }
    }
}
