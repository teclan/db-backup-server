package db.sys.server.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javalite.activejdbc.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import db.sys.server.handle.Handler;
import db.sys.server.utils.Objects;
import db.sys.server.utils.SqlGenerateUtils;

public class MysqlDatabase {
	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDatabase.class);
	private static final int LIMIT = 1000;

	private String driver;
	private String url;
	private String dbName;
	private String user;
	private String password;
	private String connectionName;
	private List<String> allTables;
	private DB db;
	private  Map<String,List<String>> pkInfo = new HashMap<String,List<String>>();
	private List<String> unSysTables;

	public MysqlDatabase(String driver, String url, String user, String password, String connectionName,List<String> unSysTables) {
		this.driver = driver;
		this.url = url;
		this.dbName = getDbName();
		this.user = user;
		this.password = password;
		this.connectionName = connectionName;
		this.unSysTables=unSysTables==null?new ArrayList<String>():unSysTables;
	}

	public void init() {
		openDatabase();
		analyzeAllTables();
		analyzePkInfo();
		closeDatabase();
	}

	public List<String> getAllTables() {
		return allTables;
	}
	
	
	public void analyzePkInfo(){
	    for(String table:allTables){
	        
	        if(unSysTables.contains(table)){
	            LOGGER.info("表 {} 不需要同步 ... ",table);
	            continue;
	        }
	        pkInfo.put(table, getPksByTable(table));
	    }
	}
	
	public Map<String,List<String>> getPkInfo(){
	    return pkInfo;
	}
	
	
	
	@SuppressWarnings("rawtypes")
    public List<String> getPksByTable(String table){
	    
	    if(pkInfo.containsKey(table)){
	        return pkInfo.get(table);
	    }
	    
	    String sql = "select concat(c.column_name) as 'column_name' from information_schema.table_constraints as t,information_schema.key_column_usage as c where t.table_name=c.table_name and t.table_name='%s' and t.table_schema='%s' and c.table_schema=t.table_schema";
	
	    List<Map> result = getDb().findAll(String.format(sql, table,dbName));
	    
	    List<String> pks = new ArrayList<String>();
	    
	    if(Objects.isNotNull(result)){
	        for(Map map:result){
	            pks.add(map.get("column_name").toString());
	        }
	    }
	   
	    return pks;
	}

	@Override
	public String toString() {
		return String.format("driver:%s\nurl:%s\nuser:%s\npassword:%s\n", driver, url, user, password);
	}

	public boolean openDatabase() {
		try {
			new DB(connectionName).open(driver, url, user, password);
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean closeDatabase() {
		try {
			if (new DB(connectionName).hasConnection()) {
				new DB(connectionName).close();
			}
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	@SuppressWarnings("rawtypes")
	private List<String> analyzeAllTables() {
		List<String> tables = new ArrayList<String>();
		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type='BASE TABLE'";
		List<Map> list = getDb().findAll(query, dbName);

		for (Map map : list) {
			tables.add(map.get("table_name").toString());
		}
		
	    tables.removeAll(unSysTables);
		allTables= tables;
		return tables;
	}

	public long count(String table) {
		return getDb().count(table);
	}
	
	@SuppressWarnings("rawtypes")
    public long count(String table,Map map) {
	    
	    List<String> pks = getPksByTable(table);
	    
	    String query = SqlGenerateUtils.getExactQuerySql(pks );
	    Object[] values =SqlGenerateUtils.getValues(map, pks );
	    
        return getDb().count(table, query, values);
    }


	public DB getDb() {
		if (db == null) {
			db = new DB(connectionName);
		}
		return db;
	}

	/**
	 * 遍历表
	 * 
	 * @param index
	 * @param handler
	 */
	@SuppressWarnings("rawtypes")
	public void traverse(String table, Handler handler) {

		try {
			long total = count(table);

			int pages = (int) Math.ceil(total * 1.0 / LIMIT);

			String sql = "select * from %s limit %s,%s";

			LOGGER.info("开始同步表 `{}`，共`{}`记录...", table, total);
			for (int i = 1; i <= pages; i++) {

				List<Map> list = getDb().findAll(String.format(sql, table, (i - 1) * LIMIT, LIMIT));

				for (Map map : list) {
					handler.handler(table, map);
				}

				if (i % 10 == 0) {
					LOGGER.info("同步表 `{}`，完成`{}`，剩余`{}`...", table, i * LIMIT > total ? total : i * LIMIT,
							i * LIMIT > total ? 0 : total - i * LIMIT);

				}
			}
			LOGGER.info("表 `{}`，同步完成...", table);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}


	private String getDbName() {
		String[] splits = url.split("\\:");
		String dbName = splits[3].substring(splits[3].indexOf("/") + 1, splits[3].indexOf("?"));
		return dbName;
	}
}
