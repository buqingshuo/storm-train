/**
 * 
 */
package storm.train.topology.ackfail;

import java.util.Map;

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
import org.apache.storm.utils.Utils;

/**
 * ����ack/failȷ�ϻ���
 * 
 * @author buqingshuo
 * @date 2020��2��15��
 */
public class LocalAckerStormTopology {

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
			++number;
			this.collector.emit(new Values(number), number);
			System.out.println("Spout: " + number);
			// ��ֹ���ݲ���̫��
			Utils.sleep(1000);
		}

		@Override
		public void ack(Object msgId) {
			System.out.println("ack invoked, msgId : " + msgId);
		}

		@Override
		public void fail(Object msgId) {
			System.out.println("fail invoked, msgId : " + msgId);
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

		private OutputCollector collector;

		/**
		 * ��ʼ��������ֻ��ִ��һ��
		 * 
		 * @param stormConf
		 * @param context
		 * @param collector
		 */
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		// int sum = 0;

		/**
		 * Ҳ��һ����ѭ���ķ�����ְ�𣺻�ȡSpout���͹��������ݲ�����
		 */
		@Override
		public void execute(Tuple input) {
			// Bolt�л�ȡֵ���Ը���index��ȡ��Ҳ���Ը�����һ�������ж����field�����ƻ�ȡ������ʹ�ã�
			Integer num = input.getIntegerByField("num");
			// sum += num;
			// System.out.println("thread : " + Thread.currentThread().getName());
			// System.out.println("Bolt: sum = [" + sum + "]");
			// ����numС��10Ϊ�ɹ�������10Ϊʧ��
			if (num > 0 && num < 11) {
				this.collector.ack(input);
			} else {
				this.collector.fail(input);
			}
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
		builder.setSpout("DataSourceSpout", new DataSourceSpout());
		builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

		// ����һ������Storm��Ⱥ������ģʽ���У�����Ҫ�Storm��Ⱥ
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LocalAckerStormTopology", new Config(), builder.createTopology());
	}
}
