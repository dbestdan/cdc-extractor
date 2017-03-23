import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public interface Config {

	public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	public final static Map<String,String> tables = new HashMap<String,String>(){{
						put("all","'bmsql_customer', "
									+ "'bmsql_history', "
									+ "'bmsql_item', "
									+ "'bmsql_new_order',"
									+ "'bmsql_oorder',"
									+ "'bmsql_order_line'"
									+ "'bmsql_stock'");
						put("first","'bmsql_customer',"
									+ "'bmsql_history',"
									+ "'bmsql_item',"
									+ "'bmsql_new_order',"
									+ "'bmsql_oorder',"
									+ "'bmsql_stock'");
						put("second","'bmsql_order_line'");
	}};
	/**
	 * 	public  String[] all = {"bmsql_customer", 
								"bmsql_history",
								"bmsql_item",
								"bmsql_new_order",
								"bmsql_oorder",
								"bmsql_order_line",
								"bmsql_stock"};
	public String[] first = {"bmsql_customer", 
							"bmsql_history",
							"bmsql_item",
							"bmsql_new_order",
							"bmsql_oorder",
							"bmsql_stock"};
	public String[] second ={"bmsql_new_order"};
 	
	
	public final static Map<String,String[]> tables = new HashMap<String,String[]>(){{
						put("all",all);
						put("first",first);
						put("second",second);
	}};
	 */
}
