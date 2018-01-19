package db.sys.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import db.sys.server.handle.Handler;
import db.sys.server.handle.impl.DefaultMysqlHandler;
import db.sys.server.mysql.MysqlFactory;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);


	public static void main(String[] args) {

		// 加载配置文件
		File file = new File("config/application.conf");
		Config root = ConfigFactory.parseFile(file);

		Config config = root.getConfig("config");


		if (MysqlFactory.init(config)) {
     	    syncMysql();
		} 

	}

	
	/**
	 * 同步 mysql 逻辑
	 */
	private static void syncMysql() {

	    LOGGER.info("开始同步 ...");
		final Handler handler = new DefaultMysqlHandler();

		ExecutorService executors = MysqlFactory.getExecutors();

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

		for (final String table : MysqlFactory.RES.getAllTables()) {
			Future<Boolean> future = executors.submit(new Callable<Boolean>() {

				public Boolean call() throws Exception {
					try {
						MysqlFactory.RES.openDatabase();
						MysqlFactory.DES.openDatabase();

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
		
		LOGGER.info("同步完成 ... ");

	}

}
