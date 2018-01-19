package db.back.server.handle;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public interface Handler {

	void handle(String index, String type, String id, JSONObject document);

	void handler(String table, Map map);
}
