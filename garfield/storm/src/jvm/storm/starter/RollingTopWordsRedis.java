/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.PublishRedisBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.RedisPubSubSpout;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWordsRedis {

  private static final Logger LOG = Logger.getLogger(RollingTopWordsRedis.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 200;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public RollingTopWordsRedis(String topologyName) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

	public static class WordSplitterBolt extends BaseBasicBolt{
	
		List<String> stopwords = new ArrayList<String>();
		
		
		public WordSplitterBolt(){
			try {
				File file = new File("./stopword.txt");
				FileReader fr = new FileReader("./stopword.txt");
				BufferedReader br = new BufferedReader(fr);
				
				String line = br.readLine();
				while( line != null ){
					stopwords.add(line.toLowerCase().trim());
					line = br.readLine();
				}

				System.out.println("==================== SIZE : " + stopwords.size() + "=============");
				System.out.println("====first word : " + stopwords.get(0));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

		
		String[] symbols = "! ? , . & ( ) { } ' \\ \" : ;".split(" ");
		
		public String filter(String word){
			word = word.trim();
			for (String sym: symbols){
				word = word.replace(sym, "");
			}
			if ( word.length() < 3  || stopwords.contains(word))
				return "";
			
			return word;
		}
		

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String sentence = input.getString(0);
			String[] words = sentence.split(" ");
			for(String word: words){
				word = filter(word);
				if(!word.isEmpty() ){
					word = word.toLowerCase();
					collector.emit(new Values(word));
				}
			}
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
 

  private void wireTopology() throws InterruptedException {
    String counterId = "counter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
    builder.setSpout("spout", new RedisPubSubSpout("localhost",6379,"twitter-stream"), 1);
    builder.setBolt("splitter", new WordSplitterBolt(),3).shuffleGrouping("spout");
	builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping("splitter", new Fields("word"));
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields(
        "obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
  	builder.setBolt("publish", new PublishRedisBolt("localhost",6379,"topwords")).globalGrouping(totalRankerId);
  }

  public void runLocally() throws InterruptedException {
    LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("word-count", topologyConfig, builder.createTopology());
	//StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  /**
   * Submits (runs) the topology.
   *
   * Usage: "RollingTopWords [topology-name] [local|remote]"
   *
   * By default, the topology is run locally under the name "slidingWindowCounts".
   *
   * Examples:
   *
   * <pre>
   * {@code
   *
   * # Runs in local mode (LocalCluster), with topology name "slidingWindowCounts"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords
   *
   * # Runs in local mode (LocalCluster), with topology name "foobar"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar
   *
   * # Runs in local mode (LocalCluster), with topology name "foobar"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar local
   *
   * # Runs in remote/cluster mode, with topology name "production-topology"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords production-topology remote
   * }
   * </pre>
   *
   * @param args First positional argument (optional) is topology name, second positional argument (optional) defines
   *             whether to run the topology locally ("local") or remotely, i.e. on a real cluster ("remote").
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String topologyName = "slidingWindowCounts";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    LOG.info("Topology name: " + topologyName);
    RollingTopWordsRedis rtw = new RollingTopWordsRedis(topologyName);
    //if run locally it will stop after a certain amount of time
	if (runLocally) {
      LOG.info("Running in local mode");
      rtw.runLocally();
    }
    else {
      LOG.info("Running in remote (cluster) mode");
      rtw.runRemotely();
    }
  }
}
