/**
 * 
 */
package storm.train.topology;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 使用Storm完成词频统计功能
 * 
 * @author buqingshuo
 * @date 2020年2月15日
 */
public class LocalCountWordStormTopology {

	public static class DataSourceSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		/**
		 * 业务： 1）读取指定目录下的数据 ；2）把每一行数据发射出去
		 */
		@Override
		public void nextTuple() {
			// 获取所有内容
			Collection<File> files = FileUtils.listFiles(new File("D:\\98Download\\test"), new String[] { "txt" },
					true);
			for (File file : files) {
				try {
					// 获取文件中的所有内容
					List<String> lines = FileUtils.readLines(file);
					// 获取文件中的每行内容
					for (String line : lines) {
						// 发射出去
						this.collector.emit(new Values(line));
					}
					// 数据处理完后改名，否则一直重复执行
					FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
	}

	/**
	 * 对数据进行分割
	 * 
	 * @author buqingshuo
	 * @date 2020年2月15日
	 */
	public static class SplitBolt extends BaseRichBolt {

		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		/**
		 * 业务逻辑：按照逗号，对line进行分割
		 */
		@Override
		public void execute(Tuple input) {
			String line = input.getStringByField("line");
			String[] words = line.split(",");
			for (String word : words) {
				collector.emit(new Values(word));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	/**
	 * 词频汇总Bolt
	 * 
	 * @author buqingshuo
	 * @date 2020年2月15日
	 */
	public static class CountBolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		}

		Map<String, Integer> map = new HashMap<>();

		/**
		 * 业务逻辑：统计词频
		 */
		@Override
		public void execute(Tuple input) {
			String word = input.getStringByField("word");
			Integer count = map.get(word);
			if (Objects.isNull(count)) {
				count = 0;
			}
			count++;
			map.put(word, count);

			map.forEach((k, v) -> System.out.println(k + ": " + v));
			System.out.println("==========");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}

	}

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("DataSourceSpout", new DataSourceSpout());
		builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
		builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LocalCountWordStormTopology", new Config(), builder.createTopology());
	}
}
