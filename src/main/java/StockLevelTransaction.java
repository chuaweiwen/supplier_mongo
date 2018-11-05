package main.java;


import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


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
        List<Document> districts = (List<Document>) targetWarehouse.get(Warehouse.W_DISTRICTS);

        Document targetDistrict = null;
        for (Document eachDistrict : districts) {
            if (eachDistrict.getInteger(District.D_ID) == D_ID) {
                targetDistrict = eachDistrict;
                break;
            }
        }

        D_NEXT_O_ID = targetDistrict.getInteger(District.D_NEXT_O_ID);
        //System.out.println(D_NEXT_O_ID);

        // 2. Let S denote the set of items from the last L orders for district (W ID,D ID); i.e.,
        //int c = 1;

        if ((D_NEXT_O_ID - L) > 0) {

            for (int i = (D_NEXT_O_ID - L); i < D_NEXT_O_ID; i++) {
                //System.out.println(c++ + ". " + i);
                Document orderOrderlineSearchQuery = new Document();
                orderOrderlineSearchQuery.append(Order.O_W_ID, W_ID);
                orderOrderlineSearchQuery.append(Order.O_D_ID, D_ID);
                orderOrderlineSearchQuery.append(Order.O_ID, i);

                Document targetOrderOrderline = orderOrderLineCollection.find(orderOrderlineSearchQuery).first();
                if (targetOrderOrderline == null) {
                    System.out.println("Oops, no such O_ID - D_NEXT_O_ID: " + D_NEXT_O_ID + ", O_ID: " + i);
                }

                List<Document> targetOrderlines = (List<Document>) targetOrderOrderline.get(Order.O_ORDERLINES);

                for (Document eachOrderline : targetOrderlines) {
                    int OL_I_ID = eachOrderline.getInteger(OrderLine.OL_I_ID);

                    Document stockSearchQuery = new Document();
                    stockSearchQuery.append(Stock.S_W_ID, W_ID);
                    stockSearchQuery.append(Stock.S_I_ID, OL_I_ID);

                    // 3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
                    MongoCursor<Document> stockCursor = stockTableCollection.find(stockSearchQuery).iterator();
                    while (stockCursor.hasNext()) {
                        Document stockDocument = stockCursor.next();
                        double quantity = 0.0;
                        try {
                            quantity = stockDocument.getDouble(Stock.S_QUANTITY);
                        } catch (ClassCastException e) {
                            quantity = (double) stockDocument.getInteger(Stock.S_QUANTITY);
                        }
                        if (quantity < (double) T) {
                            count++;
                        }
                    }
                }
            }

            System.out.println(count);

        } else {
            System.out.println("Oops, Warehouse " + W_ID + " District " + D_ID + " have less than " + L + " orders.");
        }
    }
}
