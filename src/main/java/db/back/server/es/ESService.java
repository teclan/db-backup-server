package db.back.server.es;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import db.back.server.handle.Handler;

public class ESService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ESService.class);
	private static final int LIMIT = 1000;

	private String host;
	private String port;
	private String name;
	private BulkProcessor bulkProcessor;
	public Client client;

	public ESService(String host, String port, String name) {
		this.host = host;
		this.port = port;
		this.name = name;
	}

	public void init() {
		try {
			Settings settings = Settings.settingsBuilder().put("cluster.name", name)
					.put("client.transport.ignore_cluster_name", false).put("node.client", true)
					.put("client.transport.sniff", true).build();
			try {
				client = TransportClient.builder().settings(settings).build().addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName(host), Integer.valueOf(port)));

				bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
					}

					public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
						// LOGGER.info("提交至目标ES成功");
					}

					public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
						LOGGER.info("提交至目标ES失败，{}", failure.getMessage());
					}
				}).setBulkActions(1000).setBulkSize(new ByteSizeValue(512, ByteSizeUnit.MB))
						.setFlushInterval(TimeValue.timeValueSeconds(10)).build();

			} catch (Exception e1) {
				LOGGER.error(e1.getMessage(), e1);
			}

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public Set<String> getAllIndexs() {
		ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
		Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
		Set<String> set = indexStatsMap.keySet();

		return set;
	}

	@SuppressWarnings("deprecation")
	public long countIndex(String index) {

		// SearchResponse response = client.prepareSearch(index).setTypes(type)
		// // .setQuery(new TermQueryBuilder("id", id))// 设置查询类型
		// .setSearchType(SearchType.COUNT)// 设置查询类型，有的版本可能过期
		// .setSize(0)// 设置返回结果集为0
		// .get();
		// return response.getHits().totalHits();

		CountRequestBuilder countRequestBuilder = client.prepareCount(index);

		CountResponse countResponse = countRequestBuilder.execute().actionGet();

		return countResponse.getCount();
	}

	/**
	 * 遍历索引
	 * 
	 * @param index
	 * @param handler
	 */
	public void traverse(String index, Handler handler) {
		try {
			// BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			// SortBuilder sort = new
			// FieldSortBuilder("eventTime").order(SortOrder.valueOf("DESC"));
			// boolQuery.must(QueryBuilders.termQuery("devId", devId));

			LOGGER.info("开始同步索引：{}", index);

			long total = countIndex(index);

			int pages = (int) Math.ceil(total * 1.0 / LIMIT);

			for (int i = 1; i <= pages; i++) {
				SearchResponse searchResponse = client.prepareSearch(index).setFrom((i - 1) * LIMIT).setSize(LIMIT)
						.execute().actionGet();

				SearchHits hits = searchResponse.getHits();
				SearchHit[] searchHits = hits.getHits();

				for (SearchHit hit : searchHits) {
					String id = hit.getId();
					String type = hit.getType();

					String source = hit.getSourceAsString();
					JSONObject document = JSONObject.parseObject(source);

					handler.handle(index, type, id, document);
				}

				if (i % 10 == 0) {
					LOGGER.info("索引:{},共有记录:{},已完成：{}", index, total, i * LIMIT);
				}
			}
			LOGGER.info("索引:{},共有记录:{},同步完成...", index, total);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public boolean createDocument(String index, String type, String id, JSONObject document) {

		// IndexResponse response = client.prepareIndex(index, type,
		// id).setSource(document).execute().actionGet();
		// if (response.getId() != null) {
		// return true;
		// }
		// return false;

		bulkProcessor.add(new IndexRequest(index, type, id).source(document));

		return true;
	}

	@Override
	public String toString() {
		return String.format("host:%s\tport:%s", host, port);
	}

}
