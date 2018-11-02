package main.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

public class RelatedCustomers {
    private MongoDatabase database;
    private static String ORDER_TABLE = main.java.Table.ORDER;

    RelatedCustomers(MongoDatabase database) {
        this.database = database;
    }

    void processRelatedCustomer(int w_id,int d_id,int c_id) {
        MongoCollection<Document> ordersTable = database.getCollection(ORDER_TABLE);
        HashMap <String,Integer> customerWithItem;
        HashMap<String,ArrayList<Integer>> keyToCust;
        HashMap<Integer,MongoCursor<Document>> storedIntermeResults = new HashMap<>();
        //Get all the orders from the main customer
        Document ordersSearchQuery = new Document();
        ordersSearchQuery.append(main.java.Order.O_W_ID, w_id);
        ordersSearchQuery.append(main.java.Order.O_D_ID, d_id);
        ordersSearchQuery.append(main.java.Order.O_C_ID, c_id);

        MongoCursor<Document> orderCursor = ordersTable.find(ordersSearchQuery).iterator();
        while(orderCursor.hasNext()) { //For each order go through all the orderlines and check with the other customers for similar item
            customerWithItem = new HashMap<>();
            keyToCust = new HashMap<>();
            Document orderDocument = orderCursor.next();
            ArrayList<Document> orderLineDocument = (ArrayList<Document>) orderDocument.get(main.java.Order.O_ORDERLINES);
            Iterator<Document> orderLineItor = orderLineDocument.iterator();
            MongoCursor<Document> sameItemOrder;
            while(orderLineItor.hasNext()) { //go thru the orderlines for each item
                Document orderLineObject = orderLineItor.next();
                int ol_i_id = orderLineObject.getInteger(main.java.OrderLine.OL_I_ID); //get the item id which is distinct
                if(storedIntermeResults.containsKey(ol_i_id)) {
                    sameItemOrder = storedIntermeResults.get(ol_i_id);
                }
                else {
                    Document sameItemSearchQuery = new Document();
                    //sameItemSearchQuery.append(main.java.Order.O_W_ID, new Document("$ne", w_id));
                    sameItemSearchQuery.append(main.java.Order.O_ORDERLINES+"."+ main.java.OrderLine.OL_I_ID, new Document("$eq", ol_i_id));
                    sameItemOrder = ordersTable.find(sameItemSearchQuery).iterator();
                    storedIntermeResults.put(ol_i_id,sameItemOrder);
                }
                //System.out.println("item id: "+ol_i_id);
                while(sameItemOrder.hasNext()) {
                    Document sameItemObj = sameItemOrder.next();
                    //System.out.println(sameItemObj.toJson());

                    int o_w_id = sameItemObj.getInteger(main.java.Order.O_W_ID);
                    int o_d_id = sameItemObj.getInteger(main.java.Order.O_D_ID);
                    int o_id = sameItemObj.getInteger(main.java.Order.O_ID);
                    String key = "w_id: "+ o_w_id + " d_id: "+ o_d_id + " o_id"+ o_id;
                    ArrayList<Integer> addArrayList = new ArrayList<Integer>();
                    addArrayList.add(0,o_w_id); //index 0 is w_id
                    addArrayList.add(1,o_d_id); //index 1 is d_id
                    addArrayList.add(2,o_id); //index 2 is o_id

                    if(customerWithItem.containsKey(key) && customerWithItem.get(key).intValue()==1) {
                        customerWithItem.put(key, customerWithItem.get(key).intValue() + 1);
                        keyToCust.put(key, addArrayList);//only add key of order when there is at least 2 items is same
                    }
                    else {
                        customerWithItem.put(key, 1);
                    }
                }
            }
            printRelatedCust(ordersTable,keyToCust,w_id,d_id,c_id);
        }
    }

    private void printRelatedCust(MongoCollection<Document> ordersTable, HashMap<String, ArrayList<Integer>> keyToCust, int wid, int did, int cid){
        Iterator it = keyToCust.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            int w_id = keyToCust.get(pair.getKey()).get(0);
            int d_id = keyToCust.get(pair.getKey()).get(1);
            int o_id = keyToCust.get(pair.getKey()).get(2);

            Document ordersSearchQuery = new Document();
            ordersSearchQuery.append(main.java.Order.O_W_ID, w_id);
            ordersSearchQuery.append(main.java.Order.O_D_ID, d_id);
            ordersSearchQuery.append(main.java.Order.O_ID, o_id);

            Document orderCursor = ordersTable.find(ordersSearchQuery).first();

            if(wid!=keyToCust.get(pair.getKey()).get(0) && did!=keyToCust.get(pair.getKey()).get(1) && cid!=orderCursor.getInteger("o_c_id"))
                System.out.println("cus_id: "+orderCursor.getInteger("o_c_id")+" district_id: "+keyToCust.get(pair.getKey()).get(1)+" warehouse_id: "+keyToCust.get(pair.getKey()).get(0));
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
}
