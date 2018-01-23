package db.sys.server;

import java.io.Console;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import db.sys.server.utils.Objects;
import db.sys.server.utils.SqlGenerateUtils;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {

		// 加载配置文件
		File file = new File("config/application.conf");
		Config root = ConfigFactory.parseFile(file);

		Config config = root.getConfig("config");

		boolean start = MysqlFactory.init(config);

		if (start) {
			syncMysql();
		}
	}

	/**
	 * 同步 mysql 逻辑
	 */
	private static void syncMysql() {

		LOGGER.info("开始同步 ...");

		before();

		if (Objects.isNullString(MysqlFactory.RES.getPlatformId())) {
			LOGGER.info("平台配置信息有误，同步无法继续 .... ");
			return;
		}

		final Handler handler = new DefaultMysqlHandler();

		ExecutorService executors = MysqlFactory.getExecutors();

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

		for (final String table : MysqlFactory.RES.getAllTables()) {
			// for (final String table : new String[] { "imm_syscode" }) {
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


		for (Future<Boolean> future : futures) {
			try {
				future.get();
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		handleArea();

		after();

		MysqlFactory.shutdown();


		LOGGER.info("同步完成 ... ");

		System.out.println("\n\n按任意键退出....");

		Console cs = System.console();

		while (cs.readLine() != null) {
			System.exit(0);
		}

	}

	private static void handleArea() {

		try {
			MysqlFactory.RES.openDatabase();
			MysqlFactory.DES.openDatabase();

			// 修改子平台的父节点为上级平台
			updateSubPlatformParentAreaId();

			// 修改权限
			addSubAreaToRoleAreaTable();

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			MysqlFactory.RES.closeDatabase();
			MysqlFactory.DES.closeDatabase();
		}
	}

	private static void before() {

		try {
			MysqlFactory.DES.openDatabase();

			MysqlFactory.DES.getDb().exec("DELETE FROM imm_area WHERE areaId=?", MysqlFactory.RES.getPlatformId());
			MysqlFactory.DES.getDb().exec("DELETE FROM imm_rolearea WHERE areaId=?", MysqlFactory.RES.getPlatformId());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			MysqlFactory.DES.closeDatabase();
		}

	}

	private static void after() {

		try {
			MysqlFactory.DES.openDatabase();

			MysqlFactory.DES.getDb().exec("DELETE FROM imm_area WHERE areaId=' ' or areaId IS NULL");
			MysqlFactory.DES.getDb().exec("DELETE FROM imm_rolearea WHERE areaId=' ' or areaId IS NULL");

			// TODO 清理角色相关的字典，主要是没用主键的表
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			MysqlFactory.DES.closeDatabase();
		}

	}

	private static void updateSubPlatformParentAreaId() {

		String sql = "update imm_area set parentAreaId=?,platformId=? where areaId=?";
		MysqlFactory.DES.getDb().exec(sql, MysqlFactory.DES.getPlatformId(), MysqlFactory.DES.getPlatformId(),
				MysqlFactory.RES.getPlatformId());

	}

	@SuppressWarnings("rawtypes")
	private static void addSubAreaToRoleAreaTable() {
		LOGGER.info("将子平台的区域添加到权限表...");

		String sql = "insert into imm_rolearea (roleId,areaId,is_data_node,is_handle_node,real_time_handel,real_time_browse,browse_handle_data_only,dataFrom) "
				+ " values (?,?,?,?,?,?,?,?)";

		// 拥有上级平台根节点的权限的角色，将拥有新增子平台节点的权限
		List<Map> roleareas = MysqlFactory.DES.getRoleAreaByAreaId(MysqlFactory.DES.getPlatformId());
		for (Map rolearea : roleareas) {

			if (MysqlFactory.DES.getDb().count("imm_rolearea", "roleId=? and areaId=? and is_data_node=?",
					rolearea.get("roleId"), MysqlFactory.RES.getPlatformId(), 1) > 0) {
				continue;
			}

			MysqlFactory.DES.getDb().exec(sql, rolearea.get("roleId"), MysqlFactory.RES.getPlatformId(), 1, 0, 0, 0,
					rolearea.get("browse_handle_data_only"), MysqlFactory.DES.getPlatformId());
		}

		List<Map> subAreas = MysqlFactory.DES.getDb()
				.findAll("select areaId,parentAreaId from imm_area where dataFrom=?", MysqlFactory.RES.getPlatformId());

		if (Objects.isNull(subAreas)) {
			LOGGER.info("目标数据库的 imm_area 未找到来自平台 {} 的数据...", MysqlFactory.RES.getPlatformId());
			return;
		}

		for (Map map : subAreas) {
			String parentAreaId = map.get("parentAreaId") == null ? null : map.get("parentAreaId").toString();

			if (Objects.isNullString(parentAreaId)) {
				continue;
			}
			roleareas = MysqlFactory.DES.getRoleAreaByAreaId(parentAreaId);

			for (Map rolearea : roleareas) {
				if (MysqlFactory.DES.getDb().count("imm_rolearea", "roleId=? and areaId=? and is_data_node=?",
						rolearea.get("roleId"), map.get("areaId"), 1) > 0) {
					continue;
				}
				MysqlFactory.DES.getDb().exec(sql, rolearea.get("roleId"), map.get("areaId"), 1, 0, 0, 0,
						rolearea.get("browse_handle_data_only"), MysqlFactory.DES.getPlatformId());
			}
		}
	}

	private static void addSubPlatformToAreaTable() {

		LOGGER.info("将子平台信息添加到区域表...");

		Map<String, Object> map = new HashMap<String, Object>();

		map.put("areaId", MysqlFactory.RES.getPlatformId());
		map.put("areaName", "子平台_" + MysqlFactory.RES.getPlatformId());
		map.put("parentAreaId", MysqlFactory.DES.getPlatformId());
		// 国标ID
		map.put("parents", "");
		// 数据来源为上级平台ID
		map.put("dataFrom", MysqlFactory.DES.getPlatformId());
		map.put("platformId", MysqlFactory.DES.getPlatformId());

		String sql = "insert into %s %s";

		MysqlFactory.DES.getDb().exec("delete from imm_area where areaId=?", MysqlFactory.RES.getPlatformId());
		MysqlFactory.DES.getDb().exec(String.format(sql, "imm_area", SqlGenerateUtils.generateSqlForInsert(map)),
				SqlGenerateUtils.getInsertValues(map));
	}

}
