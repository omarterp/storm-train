
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader buffReader;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */

    try {
      buffReader = new BufferedReader(new FileReader("data.txt"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

    String line = null;
    try {
      if(buffReader != null && (line = buffReader.readLine()) != null) {
        _collector.emit(new Values(buffReader.readLine()));
      } else if(buffReader != null && line == null) {
        Utils.sleep(10000);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
    if(buffReader != null) {
      try {
        buffReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
