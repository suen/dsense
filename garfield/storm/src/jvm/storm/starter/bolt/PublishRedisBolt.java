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
	String host;
	int port;
	String nameStream;
	
	public PublishRedisBolt(String host,int port,String nameStream){
		this.host = host;
		this.port = port;
		this.nameStream = nameStream;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Rankings rank = (Rankings)input.getValue(0);
		Jedis jedisServer = new Jedis(host,port);
		jedisServer.publish(nameStream,rank.toString());
		collector.emit(new Values("nothing"));
	}
		
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nothing"));
	}
}
 

