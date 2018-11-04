package main.java;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

class TopBalanceTransaction {
    private static String CUSTOMERS_TABLE = Table.CUSTOMER;

    private static final String MESSAGE_CUSTOMER =
            "Customer First: %s,  Middle: %s,  Last: %s, Balance: %.2f, Warehouse: %d, District: %d\n"; //

    private MongoDatabase database;

    TopBalanceTransaction(MongoDatabase database) {
        this.database = database;
    }

    void processTopBalance(){
        MongoCollection<Document> customersTable = database.getCollection(CUSTOMERS_TABLE);
        Document customersSearchQuery = new Document();
        MongoCursor<Document> customersCursor = customersTable.find(customersSearchQuery).sort(new Document(Customer.C_BALANCE,-1)).limit(10).iterator();
        while (customersCursor.hasNext()) {
            Document customersDocument = customersCursor.next();
            System.out.print(String.format(MESSAGE_CUSTOMER,
                    customersDocument.getString(Customer.C_FIRST),
                    customersDocument.getString(Customer.C_MIDDLE),
                    customersDocument.getString(Customer.C_LAST),
                    customersDocument.getDouble(Customer.C_BALANCE),
                    customersDocument.getInteger(Customer.C_W_ID),
                    customersDocument.getInteger(Customer.C_D_ID)));
        }
    }
}
