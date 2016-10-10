package apex.benchmark;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.netlet.util.DTThrowable;

import kafka.message.Message;

public class KafkaLatencyOperator extends KafkaSinglePortStringInputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaLatencyOperator.class);
      
  protected long maxTimeDiff = 0;
  protected long count = 0;
  protected double sum = 0;
  
  protected boolean logLatency = true;
  protected long currentTime;
  
  @Override
  public String getTuple(Message message)
  {
    currentTime = System.currentTimeMillis();
    
    String str = super.getTuple(message);
    if(logLatency) {
      logLatency(str);
    }
    
    return str;
  }

  
  public void logLatency(String str)
  {
    String sEventTime;
    
    JSONObject jsonObject;
    try {
      jsonObject = new JSONObject(str);
      sEventTime = jsonObject.getString("event_time");
    } catch (JSONException e) {
      throw DTThrowable.wrapIfChecked(e);
    }

    long timeDiff = currentTime - Long.valueOf(sEventTime);
    sum += timeDiff;
    maxTimeDiff = timeDiff > maxTimeDiff ? timeDiff : maxTimeDiff;
    if(++count % 1000000 == 0)
    {
      logger.warn("======== maxTimeDiff: {}; average: {}", maxTimeDiff, sum/count); 
      maxTimeDiff = 0;
    }
  }

}
