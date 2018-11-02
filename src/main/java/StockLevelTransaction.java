package main.java;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class StockLevelTransaction {
    private static String STOCK_TABLE ="stock";
    private static String ORDER_ORDERLINE ="orders";
    private static String WAREHOUSE_DISTRICT ="warehouse";

    private MongoDatabase database;

    private MongoCollection<Document> stockTableCollection;
    private MongoCollection<Document> orderOrderLineCollection;

    StockLevelTransaction(MongoDatabase database) {
        this.database = database;
    }

    /*
     * Processing steps:
     * 1. Let N denote the value of the next available order number D NEXT O ID for district (W ID,D ID)
     * 2. Let S denote the set of items from the last L orders for district (W ID,D ID); i.e.,
     * S = {t.OL I ID | t ∈ Order-Line, t.OL D ID = D ID, t.OL W ID = W ID, t.OL O ID ∈ [N−L,N)}
     * 3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
     * i.e., S QUANTITY < T
     */
    void processStockLevelTransaction(int W_ID, int D_ID, int T, int L) {

        int count = 0;
        int D_NEXT_O_ID = -1;

        Set<Integer> items = new HashSet<Integer>();
        MongoCollection<Document> warehouseDistrictCollection = database.getCollection(WAREHOUSE_DISTRICT);
        orderOrderLineCollection = database.getCollection(ORDER_ORDERLINE);
        stockTableCollection = database.getCollection(STOCK_TABLE);

        /* Old codes for reference [NOT WORKING]
        BasicDBObject warehouseDistrictSearchQuery = new BasicDBObject();
        warehouseDistrictSearchQuery.append("d_w_id", W_ID);
        warehouseDistrictSearchQuery.append("d_id", D_ID);
        */

        // 1. Let N denote the value of the next available order number D NEXT O ID for district (W ID,D ID)
        Document targetWarehouse = warehouseDistrictCollection.find(eq("w_id", W_ID)).first();
        List<Document> districts = (List<Document>) targetWarehouse.get("w_districts");
        Document targetDistrict = districts.get(D_ID-1);
        D_NEXT_O_ID = targetDistrict.getInteger("d_next_o_id");

        // 2. Let S denote the set of items from the last L orders for district (W ID,D ID); i.e.,
        for (int i = (D_NEXT_O_ID + 1 - L); i < D_NEXT_O_ID; i++) {
            BasicDBObject orderOrderlineSearchQuery = new BasicDBObject();
            orderOrderlineSearchQuery.append("o_w_id", W_ID);
            orderOrderlineSearchQuery.append("o_d_id", D_ID);
            orderOrderlineSearchQuery.append("o_id", i);

            Document targetOrderOrderline = orderOrderLineCollection.find(orderOrderlineSearchQuery).first();
            List<Document> targetOrderlines = (List<Document>) targetOrderOrderline.get("o_orderlines");

            for (Document eachOrderline : targetOrderlines) {
                int OL_I_ID = eachOrderline.getInteger("ol_i_id");

                BasicDBObject stockSearchQuery = new BasicDBObject();
                stockSearchQuery.append("s_w_id", W_ID);
                stockSearchQuery.append("s_i_id", OL_I_ID);

                MongoCursor<Document> stockCursor = stockTableCollection.find(stockSearchQuery).iterator();
                while (stockCursor.hasNext()) {
                    Document stockDocument = stockCursor.next();
                    int quantity = stockDocument.getInteger("s_quantity");
                    if(quantity < T) {
                        count++;
                    }
                }
                //items.add(OL_I_ID);
            }
        }

        // 3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
        /* Old code for reference
        for (int itemId : items) {
            BasicDBObject stockSearchQuery = new BasicDBObject();
            stockSearchQuery.append("s_w_id", W_ID);
            stockSearchQuery.append("s_i_id", itemId);

            MongoCursor<Document> stockCursor = stockTableCollection.find(stockSearchQuery).iterator();
            while (stockCursor.hasNext()) {
                Document stockDocument = stockCursor.next();
                int quantity = stockDocument.getInteger("s_quantity");
                if(quantity < T) {
                    count++;
                }
            }
        }
        */

        System.out.println(count);

    }
}
