package db.backup.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import db.back.server.es.ESFactory;
import db.back.server.handle.Handler;
import db.back.server.handle.impl.DefaultESHandler;
import db.back.server.handle.impl.DefaultMysqlHandler;
import db.back.server.mysql.MysqlFactory;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private static Timer MYSQL_TIMER = new Timer();
	private static Timer ES_TIMER = new Timer();

	public static void main(String[] args) {

		// 加载配置文件
		File file = new File("config/application.conf");
		Config root = ConfigFactory.parseFile(file);

		Config config = root.getConfig("config");

		if (ESFactory.init(config)) {
			ES_TIMER.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					LOGGER.info("ES 同步开始 ....");
					syncEs();
				}

			}, 0, ESFactory.PERIOD);
		} else {
			ES_TIMER.cancel();
		}

		if (MysqlFactory.init(config)) {

			MYSQL_TIMER.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					LOGGER.info("MYSQL 同步开始 ....");
					syncMysql();
				}

			}, 0, MysqlFactory.PERIOD);
		} else {
			MYSQL_TIMER.cancel();
		}

	}

	/**
	 * 同步 mysql 逻辑
	 */
	private static void syncMysql() {

		final Handler handler = new DefaultMysqlHandler();

		ExecutorService executors = MysqlFactory.getExecutors();

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

		for (final String table : MysqlFactory.RES.getAllTables()) {
			Future<Boolean> future = executors.submit(new Callable<Boolean>() {

				public Boolean call() throws Exception {
					try {
						MysqlFactory.RES.openDatabase();
						MysqlFactory.DES.openDatabase();

						MysqlFactory.DES.backTable(table);

						MysqlFactory.DES.cleanData(table);

						MysqlFactory.RES.traverse(table, handler);

						return true;
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						return false;
					} finally {
						MysqlFactory.RES.closeDatabase();
						MysqlFactory.DES.closeDatabase();
					}
				}
			});

			futures.add(future);
		}

		for (

		Future<Boolean> future : futures) {
			try {
				future.get();
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		MysqlFactory.shutdown();

	}

	/**
	 * 同步 ES 逻辑
	 */
	private static void syncEs() {

		final Handler handler = new DefaultESHandler();
		ExecutorService executors = ESFactory.getExecutors();

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

		for (final String index : ESFactory.RES.getAllIndexs()) {

			Future<Boolean> future = executors.submit(new Callable<Boolean>() {

				public Boolean call() throws Exception {
					try {
						ESFactory.RES.traverse(index, handler);
						return true;
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						return false;
					} finally {
						MysqlFactory.RES.closeDatabase();
						MysqlFactory.DES.closeDatabase();
					}
				}
			});

			futures.add(future);
		}

		for (Future<Boolean> future : futures) {
			try {
				future.get();
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		ESFactory.shutdown();
	}
}
