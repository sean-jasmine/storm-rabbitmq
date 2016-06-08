package main.java.com.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class SplitSentenceBolt implements IRichBolt {

	private static final long serialVersionUID = 2140673451296030534L;

	private OutputCollector collector;
	
	private static Map<String, Integer> wordCache = new HashMap<String, Integer>();
	
	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext ctx, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple tuple) {
		if (Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())) {
			return;
		}
		
		String[] split = tuple.getStringByField("sentences").split(" ");
		for (int i = 0; i < split.length; i++) {
			Integer word;
			String currentWord = split[i];
			if ((word = wordCache.get(currentWord)) != null) {
				wordCache.put(currentWord, 1);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		
	}
	
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
