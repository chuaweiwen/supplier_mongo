package main.java;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

class PopularItemTransaction {
    private static String ORDER_TABLE =Table.ORDER;

    private static final String MESSAGE_OID_DATETIME = "O_ID: %d, Date_Time : %s\n";

    private static final String MESSAGE_CUSTOMER = "Customer First: %s,  Middle: %s,  Last: %s\n";

    private static final String MESSAGE_NAME_QUANTITY = "Item name: %s, Quantity: %.2f\n";

    private static final String MESSAGE_PERCENTAGE = "Item name: %s, Percentage: %.2f\n";

    private MongoDatabase database;

    PopularItemTransaction(MongoDatabase database) {
        this.database = database;
    }

    void processPopularItem(int w_ID, int d_ID, int L) {

        //print w_id, d_id, L
        System.out.println("W_ID: " + w_ID + ", D_ID: " + d_ID + ", L: " + L);

        List<HashSet<String>> itemList = new ArrayList<> ();
        Set<String> popularItems = new HashSet<> ();

        MongoCollection<Document> ordersTable = database.getCollection(ORDER_TABLE);

        Document ordersSearchQuery = new Document();
        ordersSearchQuery.append(Order.O_W_ID, w_ID);
        ordersSearchQuery.append(Order.O_D_ID, d_ID);

        MongoCursor<Document> orderCursor = ordersTable.find(ordersSearchQuery).sort(new Document(Order.O_ENTRY_D,-1)).limit(L).iterator();


        while (orderCursor.hasNext()) {



            double maxQuantity = 0;
            HashSet<String> items = new HashSet<>();
            HashSet<String> maxItems = new HashSet<>();

            Document orderDocument = orderCursor.next();
            ArrayList<Document> orderLineDocument = (ArrayList<Document>) orderDocument.get(Order.O_ORDERLINES);
            Iterator<Document> orderLineItor = orderLineDocument.iterator();


            while(orderLineItor.hasNext()) {
                Document orderLineObject = orderLineItor.next();
                String ol_i_name = orderLineObject.getString(OrderLine.OL_I_NAME);
                items.add(ol_i_name);
                double quantity = orderLineObject.getInteger(OrderLine.OL_QUANTITY);

                if(quantity > maxQuantity) {
                    maxItems.clear();
                    maxItems.add(ol_i_name);
                    maxQuantity = quantity;
                } else if (quantity == maxQuantity) {
                    maxItems.add(ol_i_name);
                } else {

                }

            }
            popularItems.addAll(maxItems);
            itemList.add(items);

            //print o_id, o_entry_id, name and popular item
            System.out.print(String.format(MESSAGE_OID_DATETIME,
                    orderDocument.getInteger(Order.O_ID),
                    orderDocument.getString(Order.O_ENTRY_D)));


            System.out.print(String.format(MESSAGE_CUSTOMER,
                    orderDocument.getString(Order.O_C_FIRST),
                    orderDocument.getString(Order.O_C_MIDDLE),
                    orderDocument.getString(Order.O_C_LAST)));

            for(String itemName : maxItems) {
                System.out.print(String.format(MESSAGE_NAME_QUANTITY, itemName, maxQuantity));
            }

        }

        //print percentage

        for(String popItem : popularItems) {
            int count = 0;
            for(int i = 0; i < L; i++) {
                if(itemList.get(i).contains(popItem)) {
                    count++;
                }
            }

            System.out.print(String.format(MESSAGE_PERCENTAGE, popItem, count*1.0/L));


        }
    }
}
