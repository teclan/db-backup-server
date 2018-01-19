package db.back.server.es;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * ES 同步服务
 * 
 * @author teclan
 *
 *         2017年12月29日
 */
public class ESFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(ESFactory.class);

	/**
	 * 源 ES
	 */
	public static ESService RES;
	/**
	 * 目标 ES
	 */
	public static ESService DES;

	/**
	 * 间隔，默认 1s
	 */
	public static int PERIOD = 1000;
	/**
	 * 是否启动 ES 同步
	 */
	public static boolean ENABLE;

	/**
	 * 默认使用一个线程
	 */
	private static int THREAD_POOL_SIZE = 1;

	private static ExecutorService EXECUTORS = null;

	/**
	 * 源 ES 和 目标 ES 初始化
	 * 
	 * @param resEs
	 * @param desEs
	 */
	public static boolean init(ESService resEs, ESService desEs, int esThreadPoolSize, int period, boolean enable) {
		RES = resEs;
		DES = desEs;
		THREAD_POOL_SIZE = esThreadPoolSize;
		THREAD_POOL_SIZE = period;
		ENABLE = enable;

		if (!ESFactory.ENABLE) {
			LOGGER.info("ES 同步服务未启动...");
			return false;
		}

		RES.init();
		LOGGER.info("源ES：\n{} 初始化完成...", RES.toString());
		DES.init();
		LOGGER.info("目标ES：\n{} 初始化完成...", RES.toString());

		return true;
	}

	/**
	 * 解析 ES 配置
	 * 
	 * @param config
	 */
	public static boolean init(Config config) {
		// 加载 ES 同步的线程池大小
		int esThreadPoolSize = config.getInt("es.thread-pool-size");

		// 加载 ES 同步的同步间隔（配置单位是小时）
		int period = config.getInt("es.period") * 60 * 60 * 1000;

		boolean enable = config.getBoolean("es.enable");

		// 加载源 ES 配置
		Config resEsCfg = config.getConfig("es.res");
		// 加载目标 ES 配置
		Config desEsCfg = config.getConfig("es.des");

		// 解析源 ES 配置
		ESService resEs = new ESService(resEsCfg.getString("host"), resEsCfg.getString("port"),
				resEsCfg.getString("name"));

		// 解析目标 ES 配置
		ESService desEs = new ESService(desEsCfg.getString("host"), desEsCfg.getString("port"),
				desEsCfg.getString("name"));
		// 初始化 ES 配置
		return ESFactory.init(resEs, desEs, esThreadPoolSize, period, enable);

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
