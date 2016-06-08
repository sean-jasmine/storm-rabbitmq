package main.java.com.storm.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3501365814891366609L;

	private SpoutOutputCollector collector;
	
	private static final Random RAND = new Random();
	
	private static final String[] SENTENCES = new String[] {
			"This page is about how the serialization system in Storm works for versions 0.6.0 and onwards",
			"Storm used a different serialization system prior to 0.6.0 which is documented on Serialization (prior to 0.6.0)",
			"The name of a class to register. In this case, Storm will use Kryo's FieldsSerializer to serialize the class",
			"A map from the name of a class to register to an implementation of com.esotericsoftware.kryo.Serializer",
			"If Storm encounters a type for which it doesn't have a serialization registered, it will use Java serialization if possible",
			"Storm 0.7.0 lets you set component-specific configurations (read more about this at Configuration)",
			"You can turn off the behavior to fall back on Java serialization by setting the Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION config to false",
			"This is done by merging the component-specific serializer registrations with the regular set of serialization registrations",
			"but you want to declare all the serializations across all topologies in the storm.yaml files",
	}; 
	
	public void open(@SuppressWarnings("rawtypes") Map map, TopologyContext ctx, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	public void nextTuple() {
		collector.emit(new Values(SENTENCES[RAND.nextInt(SENTENCES.length - 1)]));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentences"));
	}

}
