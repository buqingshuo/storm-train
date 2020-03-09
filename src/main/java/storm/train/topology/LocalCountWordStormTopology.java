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
 * ʹ��Storm��ɴ�Ƶͳ�ƹ���
 * 
 * @author buqingshuo
 * @date 2020��2��15��
 */
public class LocalCountWordStormTopology {

	public static class DataSourceSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		/**
		 * ҵ�� 1����ȡָ��Ŀ¼�µ����� ��2����ÿһ�����ݷ����ȥ
		 */
		@Override
		public void nextTuple() {
			// ��ȡ��������
			Collection<File> files = FileUtils.listFiles(new File("D:\\98Download\\test"), new String[] { "txt" },
					true);
			for (File file : files) {
				try {
					// ��ȡ�ļ��е���������
					List<String> lines = FileUtils.readLines(file);
					// ��ȡ�ļ��е�ÿ������
					for (String line : lines) {
						// �����ȥ
						this.collector.emit(new Values(line));
					}
					// ���ݴ���������������һֱ�ظ�ִ��
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
	 * �����ݽ��зָ�
	 * 
	 * @author buqingshuo
	 * @date 2020��2��15��
	 */
	public static class SplitBolt extends BaseRichBolt {

		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		/**
		 * ҵ���߼������ն��ţ���line���зָ�
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
	 * ��Ƶ����Bolt
	 * 
	 * @author buqingshuo
	 * @date 2020��2��15��
	 */
	public static class CountBolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		}

		Map<String, Integer> map = new HashMap<>();

		/**
		 * ҵ���߼���ͳ�ƴ�Ƶ
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
