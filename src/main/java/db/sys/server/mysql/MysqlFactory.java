package db.sys.server.mysql;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * MySql 同步服务
 * 
 * @author teclan
 *
 *         2017年12月29日
 */
public class MysqlFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlFactory.class);
	/**
	 * 源数据库
	 */
	public static MysqlDatabase RES;
	/**
	 * 目标数据库
	 */
	public static MysqlDatabase DES;

	/**
	 * 间隔，默认 1s
	 */
	public static int PERIOD = 1000;

	/**
	 * 默认使用一个线程来同步数据
	 */
	public static int THREAD_POOL_SIZE = 1;

	/**
	 * 是否启用 MYSQL 同步服务
	 */

	public static boolean ENABLE;

	private static ExecutorService EXECUTORS = null;

	/**
	 * mysql 源数据库和目标数据库初始化
	 * 
	 * @param resMysql
	 * @param desMysql
	 */
	public static boolean init(MysqlDatabase resMysql, MysqlDatabase desMysql, int threadPoolSize, int period,
			boolean enable) {
		RES = resMysql;
		DES = desMysql;
		THREAD_POOL_SIZE = threadPoolSize;
		PERIOD = period;
		ENABLE = enable;

		if (!MysqlFactory.ENABLE) {
			LOGGER.info("MYSQL 同步服务未启动...");
			return false;
		}

		RES.init();
		LOGGER.info("源 mysql 数据库 \n{} 初始化完成 ...", RES);
		DES.init();
		LOGGER.info("目标 mysql 数据库\n{} 初始化完成 ...", DES);

		return true;
	}

	/**
	 * 解析MYSQL配置
	 * 
	 * @param config
	 */
	public static boolean init(Config config) {

		// 加载 mysql 同步的线程池大小
		int mysqlThreadPoolSize = config.getInt("mysql.thread-pool-size");

		// 加载 mysql 同步的同步间隔（配置单位是小时）
		int period = config.getInt("mysql.period") * 60 * 60 * 1000;

		boolean enable = config.getBoolean("mysql.enable");

		// 加载源 mysql 配置
		Config resMysqlCfg = config.getConfig("mysql.res");
		// 加载目标 mysql 配置
		Config desMysqlCfg = config.getConfig("mysql.des");

		// 解析源 mysql 配置
		MysqlDatabase resMysql = new MysqlDatabase(resMysqlCfg.getString("driver"), resMysqlCfg.getString("url"),
				resMysqlCfg.getString("user"), resMysqlCfg.getString("password"),
				resMysqlCfg.getString("connectionName"),resMysqlCfg.getStringList("un-sys-tables"));
		// 解析目标mysql 配置
		MysqlDatabase desMysql = new MysqlDatabase(desMysqlCfg.getString("driver"), desMysqlCfg.getString("url"),
				desMysqlCfg.getString("user"), desMysqlCfg.getString("password"),
				desMysqlCfg.getString("connectionName"),null);

		// 初始化 mysql 配置
		return MysqlFactory.init(resMysql, desMysql, mysqlThreadPoolSize, period, enable);
	}

	/**
	 * 获取线程池
	 * 
	 * @return
	 */
	public static ExecutorService getExecutors() {
		EXECUTORS = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		return EXECUTORS;
	}

	/**
	 * 所有任务完成后，关闭并清空线程池
	 */
	public static void shutdown() {
		EXECUTORS.shutdown();
		EXECUTORS = null;
	}

}
