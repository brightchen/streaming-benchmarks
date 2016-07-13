/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

package apex.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisJoin extends BaseOperator
{
  private transient RedisAdCampaignCache redisAdCampaignCache;
  private String redisServerHost;
  private static final transient Logger logger = LoggerFactory.getLogger(RedisJoin.class);


  public long getTimeDiff()
  {
    return timeDiff;
  }

  public void setTimeDiff(long timeDiff)
  {
    this.timeDiff = timeDiff;
  }

  private long timeDiff = 1000;


  public String getRedisServerHost()
  {
    return redisServerHost;
  }

  public void setRedisServerHost(String redisServerHost)
  {
    this.redisServerHost = redisServerHost;
  }

  public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
  {
    @Override
    public void process(Tuple tuple)
    {
      String campaign_id = redisAdCampaignCache.execute(tuple.adId);

      if (campaign_id == null || campaign_id.isEmpty()) {
        throw new RuntimeException("hello");
      }

      tuple.campaignId = campaign_id;

      output.emit(tuple);
    }
  };

  public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
    this.redisAdCampaignCache.prepare();
  }
}

