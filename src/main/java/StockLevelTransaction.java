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
    private static String STOCK_TABLE = Table.STOCK;
    private static String ORDER_ORDERLINE = Table.ORDER;
    private static String WAREHOUSE_DISTRICT = Table.WAREHOUSE;

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
        Document targetWarehouse = warehouseDistrictCollection.find(eq(Warehouse.W_ID, W_ID)).first();
        List<Document> districts = (List<Document>) targetWarehouse.get("w_districts");
        Document targetDistrict = districts.get(D_ID-1);
        D_NEXT_O_ID = targetDistrict.getInteger(District.D_NEXT_O_ID);

        // 2. Let S denote the set of items from the last L orders for district (W ID,D ID); i.e.,
        for (int i = (D_NEXT_O_ID + 1 - L); i < D_NEXT_O_ID; i++) {
            BasicDBObject orderOrderlineSearchQuery = new BasicDBObject();
            orderOrderlineSearchQuery.append(Order.O_W_ID, W_ID);
            orderOrderlineSearchQuery.append(Order.O_D_ID, D_ID);
            orderOrderlineSearchQuery.append(Order.O_ID, i);

            Document targetOrderOrderline = orderOrderLineCollection.find(orderOrderlineSearchQuery).first();
            List<Document> targetOrderlines = (List<Document>) targetOrderOrderline.get(Order.O_ORDERLINES);

            for (Document eachOrderline : targetOrderlines) {
                int OL_I_ID = eachOrderline.getInteger(OrderLine.OL_I_ID);

                BasicDBObject stockSearchQuery = new BasicDBObject();
                stockSearchQuery.append(Stock.S_W_ID, W_ID);
                stockSearchQuery.append(Stock.S_I_ID, OL_I_ID);

                MongoCursor<Document> stockCursor = stockTableCollection.find(stockSearchQuery).iterator();
                while (stockCursor.hasNext()) {
                    Document stockDocument = stockCursor.next();
                    int quantity = stockDocument.getInteger(Stock.S_QUANTITY);
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
