package main.java.transaction;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import main.java.Constant;

public class NewOrderTransaction {

    private MongoDatabase database;

    public NewOrderTransaction(MongoDatabase database) {
        this.database = database;
    }

    public void processOrder(int wId, int dId, int cId, int numItems,
                             int[] itemNum, int[] supplierWarehouse, int[] qty) {
        Document warehouse = getWarehouse(wId);
        Document customer = getCustomer(wId, dId, cId);

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
    }

    private Document getWarehouse(int wId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_WAREHOUSE_DISTRICT);
        return collection.find(eq("w_id", wId)).first();
    }

    private Document getCustomer(int wId, int dId, int cId) {
        MongoCollection<Document> collection = database.getCollection(Constant.TABLE_CUSTOMER);
        return collection.find(and(
                eq("c_w_id", wId),
                eq("c_d_id", dId),
                eq("c_id", cId)
        )).first();
    }
}
