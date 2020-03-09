/**
 * 
 */
package storm.train.topology;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
import org.apache.storm.utils.Utils;

/**
 * ʹ��Stormʵ���ۼ���͵Ĳ���
 * 
 * @author buqingshuo
 * @date 2020��2��15��
 */
public class ClusterSumStormExecutorsTopology {

	/**
	 * Spout��Ҫ�̳�BaseRichSpout
	 * 
	 * ����Դ��Ҫ�������ݲ�����
	 * 
	 * @author buqingshuo
	 * @date 2020��2��15��
	 */
	public static class DataSourceSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;

		/**
		 * ��ʼ��������ֻ�ᱻ����һ��
		 * 
		 * @param conf      ���ò���
		 * @param context   ������
		 * @param collector ���ݷ�����
		 */
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		int number = 0;

		/**
		 * ��������ݣ���ʵ�������п϶��Ǵ���Ϣ�����л�ȡ����
		 * 
		 * ���������һ����ѭ������һֱ��ͣ��ִ��
		 */
		@Override
		public void nextTuple() {
			this.collector.emit(new Values(++number));
			System.out.println("Spout: " + number);
			// ��ֹ���ݲ���̫��
			Utils.sleep(1000);
		}

		/**
		 * ��������ֶ�
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
		}

	}

	/**
	 * ���ݵ��ۼ����Bolt���������ݲ�����
	 * 
	 * @author buqingshuo
	 * @date 2020��2��15��
	 */
	public static class SumBolt extends BaseRichBolt {

		/**
		 * ��ʼ��������ֻ��ִ��һ��
		 * 
		 * @param stormConf
		 * @param context
		 * @param collector
		 */
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		}

		int sum = 0;

		/**
		 * Ҳ��һ����ѭ���ķ�����ְ�𣺻�ȡSpout���͹��������ݲ�����
		 */
		@Override
		public void execute(Tuple input) {
			// Bolt�л�ȡֵ���Ը���index��ȡ��Ҳ���Ը�����һ�������ж����field�����ƻ�ȡ������ʹ�ã�
			Integer num = input.getIntegerByField("num");
			sum += num;
			System.out.println("Bolt: sum = [" + sum + "]");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

		}
	}

	public static void main(String[] args) {
		// TopologyBuilder����Spout��Bolt��������Topology
		// Storm���κ�һ����ҵ����ͨ��Topology�ķ�ʽ���ύ��
		// Topology����Ҫָ��Spout��Bolt��ִ��˳��
		TopologyBuilder builder = new TopologyBuilder();
		// ��������spout��bolt�Ĳ��жȣ�Ҳ����executor�Ĳ��ж�
		builder.setSpout("DataSourceSpout", new DataSourceSpout(), 2);
		builder.setBolt("SumBolt", new SumBolt(), 2).shuffleGrouping("DataSourceSpout");

		StormSubmitter submitter = new StormSubmitter();
		String topologyName = ClusterSumStormExecutorsTopology.class.getSimpleName();
		try {
			Config config = new Config();
			config.setNumWorkers(2);
			config.setNumAckers(0);// �����в�������Ϊ0�������ܱ�֤���ݲ��ظ�����
			submitter.submitTopology(topologyName, config, builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
	}
}
