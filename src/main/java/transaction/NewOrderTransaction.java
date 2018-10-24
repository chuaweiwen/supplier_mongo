package main.java.transaction;

import java.io.BufferedWriter;
import java.io.IOException;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class NewOrderTransaction {

    private MongoDatabase database;

    private MongoCollection<Document> tableWarehouseDistrict;
    private MongoCollection<Document> tableCustomer;
    private MongoCollection<Document> tableOrderOrderLine;
    private MongoCollection<Document> tableStock;
    private MongoCollection<Document> tableItem;

    private Document selectedWarehouseDistrict;
    private Document selectedCustomer;
    private Document selectedOrderOrderLine;
    private Document selectedStock;
    private Document selectedItem;

    private BufferedWriter writer;

    public NewOrderTransaction(MongoDatabase database) {
        this.database = database;
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            System.out.println("NewOrderTransaction - Error in closing writer");
            e.printStackTrace();
        }
    }
}
