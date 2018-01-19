package db.back.server.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.javalite.activejdbc.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import db.back.server.handle.Handler;

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

	public MysqlDatabase(String driver, String url, String user, String password, String connectionName) {
		this.driver = driver;
		this.url = url;
		this.dbName = getDbName();
		this.user = user;
		this.password = password;
		this.connectionName = connectionName;
	}

	public void init() {
		openDatabase();
		allTables = analyzeAllTables();
		closeDatabase();
	}

	public List<String> getAllTables() {
		return allTables;
	}

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
		return tables;
	}

	public long count(String table) {
		return getDb().count(table);
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
				LOGGER.info("同步表 `{}`，完成`{}`，剩余`{}`...", table, i * LIMIT > total ? total : i * LIMIT,
						i * LIMIT > total ? 0 : total - i * LIMIT);
			}
			LOGGER.info("表 `{}`，同步完成...", table);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * 创建表 `table` 的备份
	 * 
	 * @param table
	 */
	public void backTable(String table) {

		String bakTabke = getBakTable(table);

		// 如果存在与备份表同名的表，则删除
		dropTableIfNeed(bakTabke);

		try {
			// 创建表 `table` 的备份表 `table`-bak
			getDb().exec(String.format("CREATE TABLE %s LIKE %s", bakTabke, table));
			// 复制表 `table` 的数据导 `table`-bak
			getDb().exec(String.format("INSERT INTO %s SELECT * FROM %s", bakTabke, table));

			LOGGER.info("目标数据库，备份表 `{}` 至表 `{}` 完成 ...", table, bakTabke);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * 清空表 `table` 的数据
	 * 
	 * @param table
	 */
	public void cleanData(String table) {
		try {
			getDb().exec(String.format("delete FROM %s", table));
			LOGGER.info("目标数据库，清理表 `{}` 数据完成...", table);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * 删除备份表
	 */
	public void dropBakTable(String table) {
		String bakTabke = getBakTable(table);
		dropTableIfNeed(bakTabke);
		LOGGER.info("目标数据库，删除表`{}`的备份表`{}`完成...", table, bakTabke);
	}

	private String getBakTable(String table) {
		return table + "_bak";
	}

	/**
	 * 如果表存在，则删除
	 * 
	 * @param table
	 */
	private void dropTableIfNeed(String table) {
		try {
			if (allTables.contains(table)) {
				getDb().exec(String.format("drop table %s", table));
			}
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
