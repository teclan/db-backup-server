package db.back.server.handle.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import db.back.server.es.ESFactory;
import db.back.server.handle.Handler;

/**
 * 对源 ES 遍历出来的每个文档的处理，默认写入目标ES，索引，类型和编号保持与源文档一致
 * 
 * @author teclan
 *
 *         2017年12月29日
 */
public class DefaultESHandler implements Handler {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultESHandler.class);

	public void handle(String index, String type, String id, JSONObject document) {
		try {
			boolean cteated = ESFactory.DES.createDocument(index, type, id, document);
			if (!cteated) {
				LOGGER.error("创建失败:{}", String.format("%s-%s-%s", index, type, id));
			} else {
				// LOGGER.error("同步完成:{}", String.format("%s-%s-%s", index, type, id));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}

	public void handler(String table, Map map) {
		// TODO Auto-generated method stub

	}
}
