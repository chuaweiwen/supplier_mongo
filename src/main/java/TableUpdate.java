package main.java;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
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
        createIndexes();
        updateCustomerLastOrder();
        combineWarehouseAndDistrict();
        addItemNameToOrderLine();
        combineOrderAndOrderLine();
    }

    private void createIndexes() {
        System.out.println("Creating index for " + Table.WAREHOUSE);
        database.getCollection(Table.WAREHOUSE).createIndex(new Document(Warehouse.W_ID,1));

        System.out.println("Creating index for " + Table.CUSTOMER);
        database.getCollection(Table.CUSTOMER).createIndex(new Document(Customer.C_W_ID, 1).append(Customer.C_D_ID, 1).append(Customer.C_ID, 1));
        database.getCollection(Table.CUSTOMER).createIndex(new Document(Customer.C_W_ID, 1));
        database.getCollection(Table.CUSTOMER).createIndex(new Document(Customer.C_D_ID, 1));
        database.getCollection(Table.CUSTOMER).createIndex(new Document(Customer.C_ID, 1));
        database.getCollection(Table.CUSTOMER).createIndex(new Document(Customer.C_BALANCE, -1));

        System.out.println("Creating index for " + Table.ORDERLINE);
        database.getCollection(Table.ORDERLINE).createIndex(new Document(OrderLine.OL_W_ID,1).append(OrderLine.OL_D_ID, 1).append(OrderLine.OL_O_ID, 1));
        database.getCollection(Table.ORDERLINE).createIndex(new Document(OrderLine.OL_W_ID,1));
        database.getCollection(Table.ORDERLINE).createIndex(new Document(OrderLine.OL_D_ID,1));
        database.getCollection(Table.ORDERLINE).createIndex(new Document(OrderLine.OL_O_ID,1));

        // temporary index to add item name
        database.getCollection(Table.ORDERLINE).createIndex(new Document(OrderLine.OL_I_ID,1));

        System.out.println("Creating index for " + Table.ORDER);
        database.getCollection(Table.ORDER).createIndex(new Document(Order.O_W_ID,1).append(Order.O_D_ID, 1).append(Order.O_ID, 1));
        database.getCollection(Table.ORDER).createIndex(new Document(Order.O_W_ID,1));
        database.getCollection(Table.ORDER).createIndex(new Document(Order.O_D_ID,1));
        database.getCollection(Table.ORDER).createIndex(new Document(Order.O_ID,1));

        System.out.println("Creating index for " + Table.STOCK);
        database.getCollection(Table.STOCK).createIndex(new Document(Stock.S_W_ID,1).append(Stock.S_I_ID,1));
        database.getCollection(Table.STOCK).createIndex(new Document(Stock.S_W_ID,1));
        database.getCollection(Table.STOCK).createIndex(new Document(Stock.S_I_ID,1));

        System.out.println("Indexes have been successfully created.");
    }

    private void updateCustomerLastOrder() {
        System.out.println("Updating " + Table.CUSTOMER + " last order");

        MongoCollection orders = database.getCollection(Table.ORDER);
        MongoCollection customerCollection = database.getCollection(Table.CUSTOMER);

        FindIterable findIterable = orders.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();
        int i = 0;
        long time = System.nanoTime();
        while (cursor.hasNext()) {
            Document orderObject = (Document) cursor.next();

            int wId = orderObject.getInteger(Order.O_W_ID);
            int dId = orderObject.getInteger(Order.O_D_ID);
            int cId = orderObject.getInteger(Order.O_C_ID);
            int oId = orderObject.getInteger(Order.O_ID);
            String entryD = orderObject.getString(Order.O_ENTRY_D);
            int carrierId = 0;
            boolean isNull = false;
            try {
                carrierId = orderObject.getInteger(Order.O_CARRIER_ID);
            } catch (ClassCastException e) {
                isNull = true;
            }

            Document searchQuery = new Document();
            searchQuery.put(Customer.C_W_ID, wId);
            searchQuery.put(Customer.C_D_ID, dId);
            searchQuery.put(Customer.C_ID, cId);

            Document newDocument = new Document();
            Document updateQuery = new Document("$set", newDocument);
            newDocument.put(Customer.C_LAST_O_ID, oId);
            newDocument.put(Customer.C_LAST_O_ENTRY_D, entryD);

            if (isNull) {
                newDocument.put(Customer.C_LAST_O_CARRIER_ID, "null");
            } else {
                newDocument.put(Customer.C_LAST_O_CARRIER_ID, carrierId);
            }

            customerCollection.updateOne(searchQuery, updateQuery);

            i++;
            if (i % 3000 == 0) {
                System.out.println((Math.round(((i / 300000.0) * 100))) + "% " + ((System.nanoTime() - time) / 1000000000));
            }
        }
        System.out.println("Updated " + Table.CUSTOMER + " last order");
    }

    private void combineWarehouseAndDistrict() {
        System.out.println("Combining " + Table.WAREHOUSE + " and " + Table.DISTRICT);
        MongoCollection districtTable = database.getCollection(Table.DISTRICT);
        MongoCollection warehouseTable = database.getCollection(Table.WAREHOUSE);

        MongoCursor districtCursor = districtTable.find().iterator();
        while (districtCursor.hasNext()) {
            Document districtObject = (Document) districtCursor.next();
            Document district = new Document();
            district.put(District.D_ID, districtObject.getInteger(District.D_ID));
            district.put(District.D_NAME, districtObject.getString(District.D_NAME));
            district.put(District.D_STREET_1, districtObject.getString(District.D_STREET_1));
            district.put(District.D_STREET_2, districtObject.getString(District.D_STREET_1));
            district.put(District.D_CITY, districtObject.getString(District.D_CITY));
            district.put(District.D_STATE, districtObject.getString(District.D_STATE));
            district.put(District.D_ZIP, districtObject.getInteger(District.D_ZIP));
            district.put(District.D_TAX, districtObject.getDouble(District.D_TAX));
            district.put(District.D_YTD, districtObject.getDouble(District.D_YTD));
            district.put(District.D_NEXT_O_ID, districtObject.getInteger(District.D_NEXT_O_ID));

            // Search for the warehouse record
            Document warehouse = new Document();
            warehouse.put(Warehouse.W_ID, districtObject.getInteger(District.D_W_ID));

            Document updateObject = new Document();
            updateObject.put("$push", new Document(Warehouse.W_DISTRICTS,district));

            warehouseTable.updateOne(warehouse, updateObject);
        }
        System.out.println("Combined " + Table.WAREHOUSE + " and " + Table.DISTRICT);
    }

    private void addItemNameToOrderLine() {
        System.out.println("Adding " + Item.I_NAME + " to " + Table.ORDERLINE);

        MongoCollection itemTable = database.getCollection(Table.ITEM);
        MongoCollection orderLineTable = database.getCollection(Table.ORDERLINE);

        FindIterable findIterable = itemTable.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();

        int i = 0;
        long time = System.nanoTime();
        while (cursor.hasNext()) {
            Document itemObject = (Document) cursor.next();
            Document item = new Document();
            item.append(OrderLine.OL_I_NAME, itemObject.getString(Item.I_NAME));

            Document searchOrderLine = new Document();
            searchOrderLine.append(OrderLine.OL_I_ID, itemObject.getInteger(Item.I_ID));

            Document updateObj = new Document();
            updateObj.append("$set", item);

            orderLineTable.updateMany(searchOrderLine, updateObj);
            i++;
            if (i % 1000 == 0) {
                System.out.println((Math.round(((i / 100000.0) * 100))) + "% " + ((System.nanoTime() - time) / 1000000000));
            }
        }
        System.out.println("Added " + Item.I_NAME + " to " + Table.ORDERLINE);
    }

    private void combineOrderAndOrderLine() {
        System.out.println("Combining " + Table.ORDER + " and " + Table.ORDERLINE);
        MongoCollection orderTable = database.getCollection(Table.ORDER);
        MongoCollection orderLineTable = database.getCollection(Table.ORDERLINE);

        FindIterable findIterable = orderLineTable.find();
        findIterable.noCursorTimeout(true);
        MongoCursor cursor = findIterable.iterator();
        int i = 0;
        long time = System.nanoTime();
        while (cursor.hasNext()) {
            Document orderLineObject = (Document) cursor.next();
            Document orderLine = new Document();
            orderLine.put(OrderLine.OL_W_ID, orderLineObject.getInteger(OrderLine.OL_W_ID));
            orderLine.put(OrderLine.OL_D_ID, orderLineObject.getInteger(OrderLine.OL_D_ID));
            orderLine.put(OrderLine.OL_O_ID, orderLineObject.getInteger(OrderLine.OL_O_ID));
            orderLine.put(OrderLine.OL_NUMBER, orderLineObject.getInteger(OrderLine.OL_NUMBER));
            orderLine.put(OrderLine.OL_I_ID, orderLineObject.getInteger(OrderLine.OL_I_ID));
            orderLine.put(OrderLine.OL_DELIVERY_D, orderLineObject.getString(OrderLine.OL_DELIVERY_D));
            orderLine.put(OrderLine.OL_AMOUNT, orderLineObject.getDouble(OrderLine.OL_AMOUNT));
            orderLine.put(OrderLine.OL_SUPPLY_W_ID, orderLineObject.getInteger(OrderLine.OL_SUPPLY_W_ID));
            orderLine.put(OrderLine.OL_QUANTITY, orderLineObject.getInteger(OrderLine.OL_QUANTITY));
            orderLine.put(OrderLine.OL_DIST_INFO, orderLineObject.getString(OrderLine.OL_DIST_INFO));

            orderLine.put(OrderLine.OL_I_NAME, orderLineObject.getString(OrderLine.OL_I_NAME));

            Document searchOrder = new Document();
            searchOrder.put(Order.O_W_ID, orderLineObject.getInteger(OrderLine.OL_W_ID));
            searchOrder.put(Order.O_D_ID, orderLineObject.getInteger(OrderLine.OL_D_ID));
            searchOrder.put(Order.O_ID, orderLineObject.getInteger(OrderLine.OL_O_ID));

            Document updateObj = new Document();
            updateObj.put("$push", new Document(Table.ORDERLINE,orderLine));

            orderTable.updateOne(searchOrder, updateObj);
            i++;
            if (i % 3750 == 0) {
                System.out.println(((Math.round(((i / 3748796.0) * 1000)))/10.0) + "% " + ((System.nanoTime() - time) / 1000000000));
            }
        }
        System.out.println("Combined " + Table.ORDER + " and " + Table.ORDERLINE);
    }
}
