package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.util.StormRunner;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import redis.clients.jedis.Jedis;
import storm.starter.tools.Rankings;

public class PublishRedisBolt extends BaseBasicBolt{
	private String host;
	private int port;
	private String nameStream;
	
	public PublishRedisBolt(String host,int port,String nameStream){
		this.host = host;
		this.port = port;
		this.nameStream = nameStream;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Rankings rank = (Rankings)input.getValue(0);
		try{
			Jedis jedisServer = new Jedis(host,port);
			String result = rank.toString();
			result = result.replaceAll("\\[","");
			result = result.replaceAll("\\]","");
			String[] tabResultRank = result.split(",");
			for(int i = 0; i < tabResultRank.length;i++){
				String[] tabResult = tabResultRank[i].split("\\|");
				String json = "{\"word\": \"" + tabResult[0] + "\",\"count\": \"" + tabResult[1] + "\"}";
				
				//String json = tabResult[0] + ":" + tabResult[1];
				jedisServer.publish(nameStream,json);
			}
		}catch(ArrayIndexOutOfBoundsException e){
			//do nothing
		}
		collector.emit(new Values("nothing"));
	}
		
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nothing"));
	}
}
 

