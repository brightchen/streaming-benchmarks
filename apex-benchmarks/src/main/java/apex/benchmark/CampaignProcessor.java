/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import benchmark.common.advertising.CampaignProcessorCommon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.helpers.AbsoluteTimeDateFormat;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;

public class CampaignProcessor extends BaseOperator
{
  private transient CampaignProcessorCommon campaignProcessorCommon;
  private String redisServerHost;

  public String getRedisServerHost()
  {
    return redisServerHost;
  }

  public void setRedisServerHost(String redisServerHost)
  {
    this.redisServerHost = redisServerHost;
  }

  public boolean isActualLatency()
  {
    return actualLatency;
  }

  private boolean actualLatency=true;
  
  protected long highLatencyCount = 0;
  
//  protected long maxLatency = 0;
//  protected long maxLatencyInPeriod = 0;
//  protected long tupleSizeInPeriod = 0;
//  protected long periodSize = 5000000;
  
  long maxDiff = 2000;
  //DateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
  DateFormat df = new AbsoluteTimeDateFormat();
  
  long lastLogTime = 0;
  public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
  {
    @Override
    public void process(Tuple tuple)
    {
      try {
        campaignProcessorCommon.execute(tuple.campaignId, tuple.event_time);

        if (actualLatency) {

          long time = System.currentTimeMillis();
          long event = Long.parseLong(tuple.event_time);
          if (time - event >= maxDiff && event != lastLogTime) {
            maxDiff = time - event;
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(time);
            lastLogTime = event;
            logger.info("High latency - new, event time: {}, time: {}",  df.format(new Date(event)), df.format(new Date(time)));
          }
          
//          maxLatencyInPeriod = (time - event > maxLatencyInPeriod ? time - event : maxLatencyInPeriod);
//          if(++tupleSizeInPeriod >= periodSize) {
//            maxLatency = (maxLatencyInPeriod > maxLatency ? maxLatencyInPeriod : maxLatency);
//            logger.warn("maxLatencyInPeriod: {}; maxLatency: {}", maxLatencyInPeriod, maxLatency);
//            tupleSizeInPeriod = 0;
//            maxLatencyInPeriod = 0;
//          }
        }

      } catch ( Exception exception ) {
        throw new RuntimeException("" + tuple.campaignId + ", " + tuple.event_time);
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
    this.campaignProcessorCommon.prepare();
  }

  public void setActualLatency(boolean actualLatency)
  {
    this.actualLatency = actualLatency;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(CampaignProcessor.class);

}

