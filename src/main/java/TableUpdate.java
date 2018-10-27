package main.java;

import java.util.Date;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * To update the table right after the data are imported
 * to fit our data model.
 */
public class TableUpdate {
    private MongoDatabase database;

    public static void main (String[] args) {
        String[] configData = Main.readConfigFile();

        String host = configData[0];
        int port = Integer.parseInt(configData[1]);
        String databaseName = configData[2];
        String consistencyLevel = configData[3];

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
        TableUpdate tableUpdate = new TableUpdate(database);
        tableUpdate.run();
    }

    public TableUpdate(MongoDatabase database) {
        this.database = database;
    }

    public void run() {
        updateCustomerLastOrder();
        combineWarehouseAndDistrict();
        addItemNameToOrderLine();
        combineOrderAndOrderLine();
    }

    private void updateCustomerLastOrder() {
        MongoCollection<Document> orders = database.getCollection(Table.ORDER);
        MongoCollection<Document> customerCollection = database.getCollection(Table.CUSTOMER);

        FindIterable findIterable = orders.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            BasicDBObject orderObject = (BasicDBObject) cursor.next();

            int wId = orderObject.getInt(Order.O_W_ID);
            int dId = orderObject.getInt(Order.O_D_ID);
            int cId = orderObject.getInt(Order.O_C_ID);
            int oId = orderObject.getInt(Order.O_ID);
            Date entryD = orderObject.getDate(Order.O_ENTRY_D);
            String carrierId = orderObject.getString(Order.O_CARRIER_ID);

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put(Customer.C_W_ID, wId);
            searchQuery.put(Customer.C_D_ID, dId);
            searchQuery.put(Customer.C_ID, cId);

            BasicDBObject newDocument = new BasicDBObject();
            BasicDBObject updateQuery = new BasicDBObject("$set", newDocument);
            newDocument.put(Customer.C_LAST_O_ID, oId);
            newDocument.put(Customer.C_LAST_O_ENTRY_D, entryD);
            newDocument.put(Customer.C_LAST_O_CARRIER_ID, carrierId);

            customerCollection.updateOne(searchQuery, updateQuery);
        }
        System.out.println("Updated customer's last order");
    }

    private void combineWarehouseAndDistrict() {
        database.getCollection(Table.WAREHOUSE).createIndex(new BasicDBObject(Warehouse.W_ID,1));
        MongoCollection districtTable = database.getCollection(Table.DISTRICT);
        MongoCollection warehouseTable = database.getCollection(Table.WAREHOUSE);

        MongoCursor districtCursor = districtTable.find().iterator();
        while (districtCursor.hasNext()) {
            BasicDBObject districtObject = (BasicDBObject) districtCursor.next();
            BasicDBObject district = new BasicDBObject();
            district.put(District.D_ID, districtObject.getInt(District.D_ID));
            district.put(District.D_NAME, districtObject.getString(District.D_NAME));
            district.put(District.D_STREET_1, districtObject.getString(District.D_STREET_1));
            district.put(District.D_STREET_2, districtObject.getString(District.D_STREET_1));
            district.put(District.D_CITY, districtObject.getString(District.D_CITY));
            district.put(District.D_STATE, districtObject.getString(District.D_STATE));
            district.put(District.D_ZIP, districtObject.getInt(District.D_ZIP));
            district.put(District.D_TAX, districtObject.getDouble(District.D_TAX));
            district.put(District.D_YTD, districtObject.getDouble(District.D_YTD));
            district.put(District.D_NEXT_O_ID, districtObject.getInt(District.D_NEXT_O_ID));

            // Search for the warehouse record
            BasicDBObject warehouse = new BasicDBObject();
            warehouse.put(Warehouse.W_ID, districtObject.getInt(District.D_W_ID));

            BasicDBObject updateObject = new BasicDBObject();
            updateObject.put("$push", new BasicDBObject(Warehouse.W_DISTRICTS,district));

            warehouseTable.updateOne(warehouse, updateObject);
        }
        warehouseTable.renameCollection(new MongoNamespace(Table.WAREHOUSE_DISTRICT));
        System.out.println("Combined warehouse and district");
    }

    private void addItemNameToOrderLine() {
        database.getCollection(Table.ORDERLINE).createIndex(new BasicDBObject(OrderLine.OL_W_ID,1).append(OrderLine.OL_D_ID, 1).append(OrderLine.OL_O_ID, 1));
        database.getCollection(Table.ORDERLINE).createIndex(new BasicDBObject(OrderLine.OL_W_ID,1));
        database.getCollection(Table.ORDERLINE).createIndex(new BasicDBObject(OrderLine.OL_D_ID,1));
        database.getCollection(Table.ORDERLINE).createIndex(new BasicDBObject(OrderLine.OL_O_ID,1));

        //database.getCollection(Table.ORDERLINE).createIndex(new BasicDBObject(OrderLine.OL_I_ID,1));

        MongoCollection itemTable = database.getCollection(Table.ITEM);
        MongoCollection orderLineTable = database.getCollection(Table.ORDERLINE);

        FindIterable findIterable = itemTable.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();

        while (cursor.hasNext()) {
            BasicDBObject itemObject = (BasicDBObject) cursor.next();
            BasicDBObject item = new BasicDBObject();
            item.append(OrderLine.OL_I_NAME, itemObject.getString(Item.I_NAME));

            BasicDBObject searchOrderLine = new BasicDBObject();
            searchOrderLine.append(OrderLine.OL_I_ID, itemObject.getInt(Item.I_ID));

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.append("$set", item);

            orderLineTable.updateMany(searchOrderLine, updateObj);
        }
        System.out.println("Added item names to order lines");
    }

    private void combineOrderAndOrderLine() {
        MongoCollection orderTable = database.getCollection(Table.ORDER);
        MongoCollection orderLineTable = database.getCollection(Table.ORDERLINE);

        FindIterable findIterable = orderLineTable.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();

        while (cursor.hasNext()) {
            BasicDBObject orderLineObject = (BasicDBObject) cursor.next();
            BasicDBObject orderLine = new BasicDBObject();
            orderLine.put(OrderLine.OL_W_ID, orderLineObject.getInt(OrderLine.OL_W_ID));
            orderLine.put(OrderLine.OL_D_ID, orderLineObject.getInt(OrderLine.OL_D_ID));
            orderLine.put(OrderLine.OL_O_ID, orderLineObject.getInt(OrderLine.OL_O_ID));
            orderLine.put(OrderLine.OL_NUMBER, orderLineObject.getInt(OrderLine.OL_NUMBER));
            orderLine.put(OrderLine.OL_I_ID, orderLineObject.getInt(OrderLine.OL_I_ID));
            orderLine.put(OrderLine.OL_DELIVERY_D, orderLineObject.getString(OrderLine.OL_DELIVERY_D));
            orderLine.put(OrderLine.OL_AMOUNT, orderLineObject.getDouble(OrderLine.OL_AMOUNT));
            orderLine.put(OrderLine.OL_SUPPLY_W_ID, orderLineObject.getInt(OrderLine.OL_SUPPLY_W_ID));
            orderLine.put(OrderLine.OL_QUANTITY, orderLineObject.getInt(OrderLine.OL_QUANTITY));
            orderLine.put(OrderLine.OL_DIST_INFO, orderLineObject.getString(OrderLine.OL_DIST_INFO));

            orderLine.put(OrderLine.OL_I_NAME, orderLineObject.getString(OrderLine.OL_I_NAME));

            BasicDBObject searchOrder = new BasicDBObject();
            searchOrder.put(Order.O_W_ID, orderLineObject.getInt(OrderLine.OL_W_ID));
            searchOrder.put(Order.O_D_ID, orderLineObject.getInt(OrderLine.OL_D_ID));
            searchOrder.put(Order.O_ID, orderLineObject.getInt(OrderLine.OL_O_ID));

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$push", new BasicDBObject(Table.ORDERLINE,orderLine));

            orderTable.updateOne(searchOrder, updateObj);
        }
        orderTable.renameCollection(new MongoNamespace(Table.ORDER_ORDERLINE));
        System.out.println("Combined order and order-line");
    }
}
