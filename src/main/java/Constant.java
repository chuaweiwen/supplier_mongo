package main.java;

/**
 * Constants used for the application
 */
public class Constant {
    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = "27017";
    public static final String DEFAULT_DATABASE = "supplier";

    public static final String TABLE_WAREHOUSE = "warehouse";
    public static final String TABLE_DISTRICT = "district";
    public static final String TABLE_CUSTOMER = "customer";
    public static final String TABLE_ORDER = "orders";
    public static final String TABLE_ITEM = "item";
    public static final String TABLE_STOCK = "stock";
    public static final String TABLE_ORDERLINE = "orderline";

    public static final String TABLE_WAREHOUSE_DISTRICT = "warehouseDistrict";
    public static final String TABLE_ORDER_ORDERLINE = "orderOrderLine";

    public static final String CONFIGURATION_FILE = "config.env";

    public static final String PERFORMANCE_OUTPUT_PATH = "performance_output.txt";

    public static String getTransactionFileLocation(int fileNameWithoutExtension) {
        return "xact/" + fileNameWithoutExtension + ".txt";
    }
}
