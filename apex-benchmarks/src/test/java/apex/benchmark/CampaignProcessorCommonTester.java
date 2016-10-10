package apex.benchmark;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import benchmark.common.advertising.CampaignProcessorCommon;

public class CampaignProcessorCommonTester
{
  private static final transient Logger logger = LoggerFactory.getLogger(CampaignProcessorCommonTester.class);
  
  protected transient CampaignProcessorCommon campaignProcessorCommon;
  protected String redisServerHost = "localhost";
  
  protected int campaignSize = 100;
  protected int waitTime = 2;
  protected Random random = new Random();
  
  protected int runTime = 5 * 60000;
  
  @Before
  public void setup()
  {
    campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
    campaignProcessorCommon.prepare();
  }
  
  @Test
  public void test()
  {
    long maxSpentTime = 0;
    int count = 0;
    long endTime = System.currentTimeMillis() + runTime;
    while(System.currentTimeMillis() < endTime) {
      long startTime = System.currentTimeMillis();
      campaignProcessorCommon.execute(""+random.nextInt(campaignSize), ""+System.currentTimeMillis());
      long spentTime = System.currentTimeMillis() - startTime;
      maxSpentTime = spentTime > maxSpentTime ? spentTime : maxSpentTime;
      if(++count > 500) {
        logger.info("max spent time: {}", maxSpentTime);
        count = 0;
        maxSpentTime = 0;
      }
      try{
        Thread.sleep(waitTime);
      } catch(Exception e) {
        //ignore
      }
    }
  }
}
