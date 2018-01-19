package db.back.server.handle.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import db.back.server.handle.Handler;
import db.back.server.mysql.MysqlFactory;
import db.back.server.utils.SqlGenerateUtils;

/**
 * 对源 mysql 遍历出来的每个记录的处理，默认写入目标 mysql，先删除后插入
 * 
 * @author teclan
 *
 *         2017年12月29日
 */
public class DefaultMysqlHandler implements Handler {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMysqlHandler.class);

	private static String DELETE_SQL = "delete from %s where %s";

	public void handle(String index, String type, String id, JSONObject document) {
		// TODO Auto-generated method stub
	}

	@SuppressWarnings("unchecked")
	public void handler(String table, Map map) {
		String sql = "insert into %s %s";
		try {
			MysqlFactory.DES.getDb().exec(String.format(sql, table, SqlGenerateUtils.generateSqlForInsert(map)),
					SqlGenerateUtils.getInsertValues(map));

			// LOGGER.info("同步成功,表 `{}`,记录`{}`", table, map.toString());

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			LOGGER.info("同步失败,表 `{}`,记录`{}`", table, map.toString());
		}

	}

}
