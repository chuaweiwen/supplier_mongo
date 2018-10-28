package main.java;

/**
 * Constants used for the application
 */
public class Constant {
    static final String DEFAULT_HOST = "localhost";
    static final String DEFAULT_PORT = "27017";
    static final String DEFAULT_DATABASE = "supplier";
    static final String DEFAULT_CONSISTENCY_LEVEL = "local";
    static final String DEFAULT_NUMBER_OF_TRANSACTIONS = "10";

    static final String HOST_KEY = "HOST";
    static final String PORT_KEY = "PORT";
    static final String DATABASE_KEY = "DATABASE";
    static final String CONSISTENCY_LEVEL_KEY = "CONSISTENCY_LEVEL";
    static final String NUMBER_OF_TRANSACTIONS_KEY = "NUMBER_OF_TRANSACTIONS";

    static final String CONFIGURATION_FILE = "config.env";
    static final String PERFORMANCE_OUTPUT_PATH = "performance_output.txt";

    static String getTransactionFileLocation(int fileNameWithoutExtension) {
        return "xact/" + fileNameWithoutExtension + ".txt";
    }
}

class Table {
    static final String WAREHOUSE = "warehouse";
    static final String DISTRICT = "district";
    static final String CUSTOMER = "customer";
    static final String ORDER = "orders";
    static final String ITEM = "item";
    static final String STOCK = "stock";
    static final String ORDERLINE = "orderline";

    //static final String WAREHOUSE_DISTRICT = "warehouseDistrict";
    //static final String ORDER_ORDERLINE = "orderOrderLine";
}

class Warehouse {
    static final String W_ID = "w_id";
    static final String W_NAME = "w_name";
    static final String W_STREET_1 = "w_street_1";
    static final String W_STREET_2 = "w_street_2";
    static final String W_CITY = "w_city";
    static final String W_STATE = "w_state";
    static final String W_ZIP = "w_zip";
    static final String W_TAX = "w_tax";
    static final String W_YTD = "w_ytd";
    static final String W_DISTRICTS = "w_districts";
}

class District {
    static final String D_W_ID = "d_w_id";
    static final String D_ID = "d_id";
    static final String D_NAME = "d_name";
    static final String D_STREET_1 = "d_street_1";
    static final String D_STREET_2 = "d_street_2";
    static final String D_CITY = "d_city";
    static final String D_STATE = "d_state";
    static final String D_ZIP = "d_zip";
    static final String D_TAX = "d_tax";
    static final String D_YTD = "d_ytd";
    static final String D_NEXT_O_ID = "d_next_o_id";
}

class Customer {
    static final String C_W_ID = "c_w_id";
    static final String C_D_ID = "c_d_id";
    static final String C_ID = "c_id";
    static final String C_FIRST = "c_first";
    static final String C_MIDDLE = "c_middle";
    static final String C_LAST = "c_last";
    static final String C_STREET_1 = "c_street_1";
    static final String C_STREET_2 = "c_street_2";
    static final String C_CITY = "c_city";
    static final String C_STATE = "c_state";
    static final String C_ZIP = "c_zip";
    static final String C_PHONE = "c_phone";
    static final String C_SINCE = "c_since";
    static final String C_CREDIT = "c_credit";
    static final String C_CREDIT_LIM = "c_credit_lim";
    static final String C_DISCOUNT = "c_discount";
    static final String C_BALANCE = "c_balance";
    static final String C_YTD_PAYMENT = "c_ytd_payment";
    static final String C_PAYMENT_CNT = "c_payment_cnt";
    static final String C_DELIVERY_CNT = "c_delivery_cnt";
    static final String C_DATA = "c_data";

    static final String C_LAST_O_ID = "c_last_o_id";
    static final String C_LAST_O_ENTRY_D = "c_last_o_entry_d";
    static final String C_LAST_O_CARRIER_ID = "c_last_o_carrier_id";
}

class Order {
    static final String O_W_ID = "o_w_id";
    static final String O_D_ID = "o_d_id";
    static final String O_ID = "o_id";
    static final String O_C_ID = "o_c_id";
    static final String O_CARRIER_ID = "o_carrier_id";
    static final String O_OL_CNT = "o_ol_cnt";
    static final String O_ALL_LOCAL = "o_all_local";
    static final String O_ENTRY_D = "o_entry_d";

    static final String O_ORDERLINES = "o_orderlines";
}

class Item {
    static final String I_ID = "i_id";
    static final String I_NAME = "i_name";
    static final String I_PRICE = "i_price";
    static final String I_IM_ID = "i_im_id";
    static final String I_DATA = "i_data";
}

class OrderLine {
    static final String OL_W_ID = "ol_w_id";
    static final String OL_D_ID = "ol_d_id";
    static final String OL_O_ID = "ol_o_id";
    static final String OL_NUMBER = "ol_number";
    static final String OL_I_ID = "ol_i_id";
    static final String OL_DELIVERY_D = "ol_delivery_d";
    static final String OL_AMOUNT = "ol_amount";
    static final String OL_SUPPLY_W_ID = "ol_supply_w_id";
    static final String OL_QUANTITY = "ol_quantity";
    static final String OL_DIST_INFO = "ol_dist_info";

    static final String OL_I_NAME = "ol_i_name";
}

class Stock {
    static final String S_W_ID = "s_w_id";
    static final String S_I_ID = "s_i_id";
    static final String S_QUANTITY = "s_quantity";
    static final String S_YTD = "s_ytd";
    static final String S_ORDER_CNT = "s_order_cnt";
    static final String S_REMOTE_CNT = "s_remote_cnt";
    static final String S_DATA = "s_data";

    static String getDistrictStringId(int dId) {
        if (dId < 10) {
            return "s_dist_0" + dId;
        } else {
            return "s_dist_10";
        }
    }
}