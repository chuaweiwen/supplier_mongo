package main.java;

import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.List;

class PaymentTransaction {
    private MongoDatabase database;
    private MongoCollection<Document> warehouseDistrictCollection;
    private MongoCollection<Document> customerCollection;

    private static final String MESSAGE_WAREHOUSE =
            "Warehouse address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_DISTRICT =
            "District address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_CUSTOMER =
            "Customer: Identifier(%1$s, %2$s, %3$s), "
                    + "Name(%4$s, %5$s, %6$s), "
                    + "Address(%7$s, %8$s, %9$s, %10$s, %11$s), "
                    + "Phone(%12$s), Since(%13$s), "
                    + "Credits(%14$s, %15$s, %16$s, %17$s)";

    PaymentTransaction(MongoDatabase database) {
        this.database = database;
    }

    void processPaymentTransaction(int W_ID, int D_ID, int C_ID, double paymentAmount) {
        warehouseDistrictCollection = database.getCollection(Table.WAREHOUSE);
        customerCollection = database.getCollection(Table.CUSTOMER);

        updateWarehouseYTD(W_ID, paymentAmount);
        updateDistrictYTD(W_ID, D_ID, paymentAmount);
        updateCustomer(W_ID, D_ID, C_ID, paymentAmount);

        /* Output follow info
        * 1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST),
        *    address(C STREET 1, C STREET 2, C CITY, C STATE, C ZIP),
        *    C PHONE, C SINCE, C CREDIT, C CREDIT LIM, C DISCOUNT, C BALANCE
        * 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
        * 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
        * 4. Payment amount PAYMENT
        */

        System.out.println("Payment amount: " + paymentAmount);
    }

    /**
     * Decrement C_BALANCE by PAYMENT
     * Increment C_YTD_PAYMENT by PAYMENT
     * Increment C_PAYMENT_CNT by 1
     */
    private void updateCustomer(int wId, int dId, int cId, double payment) {

        Document searchCustomerQuery = new Document();
        searchCustomerQuery.append("c_w_id", wId);
        searchCustomerQuery.append("c_d_id", dId);
        searchCustomerQuery.append("c_id", cId);

        Document targetCustomer = this.customerCollection.find(searchCustomerQuery).first();

        Document carrier = new Document();
        Document set = new Document("$set", carrier);
        carrier.put("c_balance", targetCustomer.getDouble("c_balance") + payment);
        carrier.put("c_ytd_payment", targetCustomer.getDouble("c_ytd_payment") + payment);
        carrier.put("c_payment_cnt", targetCustomer.getInteger("c_payment_cnt") + 1);

        customerCollection.updateOne(searchCustomerQuery, set);
        /*
         * 1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST),
         *    address(C STREET 1, C STREET 2, C CITY, C STATE, C ZIP),
         *    C PHONE, C SINCE, C CREDIT, C CREDIT LIM, C DISCOUNT, C BALANCE
         */
        outputUpdateCustomer(targetCustomer, payment);
    }

    private void outputUpdateCustomer(Document targetCustomer, double payment) {
        System.out.println(String.format(MESSAGE_CUSTOMER,
                targetCustomer.get("c_w_id"),
                targetCustomer.get("c_d_id"),
                targetCustomer.get("c_id"),

                targetCustomer.get("c_first"),
                targetCustomer.get("c_middle"),
                targetCustomer.get("c_last"),

                targetCustomer.get("c_street_1"),
                targetCustomer.get("c_street_2"),
                targetCustomer.get("c_city"),
                targetCustomer.get("c_state"),
                targetCustomer.get("c_zip"),

                targetCustomer.get("c_phone"),
                targetCustomer.get("c_since"),

                targetCustomer.get("c_credit"),
                targetCustomer.get("c_credit_lim"),
                targetCustomer.get("c_discount"),
                (targetCustomer.getDouble("c_balance") + payment)));
    }

    // increase D_YTD by PAYMENT
    private void updateDistrictYTD(int wId, int dId, double payment) {
        //System.out.println("updating D_YTD...");

        Document searchWarehouseDistrictQuery = new Document();
        searchWarehouseDistrictQuery.append("w_id", wId);

        Document warehouse = warehouseDistrictCollection.find(searchWarehouseDistrictQuery).first();
        //System.out.println(warehouse + "\n");
        Document district = null;
        List<Document> districts = (List<Document>) warehouse.get("w_districts");

        for (Document eachDistrict : districts) {
            if (eachDistrict.getInteger("d_id") == dId) {
                district = eachDistrict;
                break;
            }
        }

        //System.out.println("District before update: "  + district);

        Document carrier = new Document();
        carrier.put("w_districts.$.d_ytd", district.getDouble("d_ytd") + payment);
        searchWarehouseDistrictQuery.put("w_districts.d_id", dId);
        Document set = new Document("$set", carrier);

        warehouseDistrictCollection.updateOne(searchWarehouseDistrictQuery, set);

        //System.out.println(warehouseDistrictCollection.find(searchWarehouseDistrictQuery).first());
        /*3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)*/
        outputUpdateDistrict(district);
    }

    private void outputUpdateDistrict(Document district) {
        System.out.println(String.format(MESSAGE_DISTRICT,
                district.get("d_street_1"),
                district.get("d_street_2"),
                district.get("d_city"),
                district.get("d_state"),
                district.get("d_zip")));
    }

    // increase W_YTD by PAYMENT
    private void updateWarehouseYTD(int wId, double payment) {
        //System.out.println("updating W_YTD...");

        Document find = new Document();
        find.append("w_id", wId);

        Document warehouse = warehouseDistrictCollection.find(find).first();

        Document carrier = new Document();
        Document set = new Document("$set", carrier);
        carrier.put("w_ytd", warehouse.getDouble("w_ytd") + payment);

        warehouseDistrictCollection.updateOne(find, set);

        /* 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)*/
        outputUpdateWarehouse(warehouse);
    }

    private void outputUpdateWarehouse(Document warehouse) {
        System.out.println(String.format(MESSAGE_WAREHOUSE,
                warehouse.get("w_street_1"),
                warehouse.get("w_street_2"),
                warehouse.get("w_city"),
                warehouse.get("w_state"),
                warehouse.get("w_zip")));
    }
}
