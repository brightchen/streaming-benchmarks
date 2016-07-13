/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import benchmark.common.advertising.CampaignProcessorCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

          if (time - event >= 2000) {

            logger.info(" High latency {}", time - event);
          }

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

