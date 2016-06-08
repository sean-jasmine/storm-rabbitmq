package main.java.com.storm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
 
//import mytest.ThroughputTest.GenSpout;
 
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
/*
 *  storm jar stormtest.jar socket.SocketProcess /home/hadoop/out_socket.txt true
 * 
 */
 
public class SocketProcess {
         public static class  SocketSpout extends BaseRichSpout {
 
                   /**
                    */
        	  static Socket sock=null;
        	  static BufferedReader in=null;
        	  String str=null;
                   private static final long serialVersionUID = 1L;
                   private SpoutOutputCollector _collector;
                   private BufferedReader br;
                   private String dataFile;
                   private BufferedWriter bw2;
                    RandomAccessFile randomFile;
                    private long lastTimeFileSize = 0; 
                    int cnt=0;
                   //定义spout文件
                    SocketSpout(){
                     
                   }
 
                   //定义如何读取spout文件
                   public void open(Map conf, TopologyContext context,
                                     SpoutOutputCollector collector) {
                            // TODO Auto-generated method stub
                            _collector = collector;
                            try {
                sock=new Socket("192.168.27.100",5678);
                 in=   
                  new BufferedReader(new InputStreamReader(sock.getInputStream()));   
              } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
                       
                   }
 
                   //获取下一个tuple的方法
                   public void nextTuple() {
                            // TODO Auto-generated method stub
                	   if(sock==null){
                		     try {
                sock=new Socket("192.168.27.100",5678);
                 in=   
                    new BufferedReader(new InputStreamReader(sock.getInputStream()));  
              } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } 
                	   }
                	   
                	   
                	   while(true){    
                		  
            try {
              str = in.readLine();
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
                		System.out.println(str);  
                		_collector.emit(new Values(str));
                		if(str.equals("end")){    
                			break;    
                			} 
                		}
                   }
 
                   public void declareOutputFields(OutputFieldsDeclarer declarer) {
                            // TODO Auto-generated method stub
                            declarer.declare(new Fields("line"));
                   }
                  
         }
        
         public static class Process extends BaseRichBolt{
 
                   private String _seperator;
                   private String _outFile;
                   PrintWriter pw;
                   private OutputCollector _collector;
                   private BufferedWriter bw;
                  
                   public Process(String outFile) {
                           
                            this._outFile   = outFile;
                           
                   }
                  
                   //把输出结果保存到外部文件里面。
                   public void prepare(Map stormConf, TopologyContext context,
                                     OutputCollector collector) {
                            // TODO Auto-generated method stub
                            this._collector = collector;
                            File out = new File(_outFile);
                            try {
//                                  br = new BufferedWriter(new FileWriter(out));
                                     bw = new BufferedWriter(new OutputStreamWriter( 
                             new FileOutputStream(out, true))); 
                            } catch (IOException e1) {
                                     // TODO Auto-generated catch block
                                     e1.printStackTrace();
                            }                
                   }
                  
                   //blot计算单元，把tuple中的数据添加一个bkeep和回车。然后保存到outfile指定的文件中。
                   public void execute(Tuple input) {
                            // TODO Auto-generated method stub
                            String line = input.getString(0);
//                         System.out.println(line);
                       //     String[] str = line.split(_seperator);
                         //   System.out.println(str[2]);
                            try {
                                     bw.write(line+",bkeep"+"\n");
                                     bw.flush();
                            } catch (IOException e) {
                                     // TODO Auto-generated catch block
                                     e.printStackTrace();
                            }
                           
                            _collector.emit(new Values(line));
                   }
 
                   public void declareOutputFields(OutputFieldsDeclarer declarer) {
                            // TODO Auto-generated method stub
                            declarer.declare(new Fields("line"));
                   }
                  
         }
        
         public static void main(String[] argv) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException{
                
                   String outFile   = argv[0]; //输出文件
                   boolean distribute = Boolean.valueOf(argv[1]);       //本地模式还是集群模式
                   TopologyBuilder builder = new TopologyBuilder();  //build一个topology
        builder.setSpout("spout", new  SocketSpout(), 1);   //指定spout
        builder.setBolt("bolt", new Process(outFile),1).shuffleGrouping("spout");  //指定bolt，包括bolt、process和grouping
        Config conf = new Config();
        if(distribute){
            StormSubmitter.submitTopology("SocketProcess", conf, builder.createTopology());
        }else{
                 LocalCluster cluster = new LocalCluster();
                 cluster.submitTopology("SocketProcess", conf, builder.createTopology());
        }
         }       
}
