/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@ApplicationAnnotation(name = "ApplicationWithGenerator2")
public class ApplicationWithGenerator2 implements StreamingApplication
{
  private static final transient Logger logger = LoggerFactory.getLogger(ApplicationWithGenerator2.class);

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
     // Create operators for each step
     // settings are applied by the platform using the config file.
    JsonGenerator2 eventGenerator = dag.addOperator("eventGenerator", new JsonGenerator2());

    //CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());

    eventGenerator.setNumAdsPerCampaign(Integer.parseInt(configuration.get("numberOfAds")));
    eventGenerator.setNumCampaigns(Integer.parseInt(configuration.get("numberOfCampaigns")));

    eventGenerator.init();
    Map<String, List<String>> campaigns = eventGenerator.getCampaigns();

    setupRedis(campaigns, configuration.get("redis"));

    logger.info(" Campaign size{}", campaigns.size());
  }

  private void setupRedis(Map<String, List<String>> campaigns, String redis)
  {
    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init(redis);

    redisHelper.prepareRedis(campaigns);
  }

}
