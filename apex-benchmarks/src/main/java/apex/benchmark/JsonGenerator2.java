/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import benchmark.common.advertising.CampaignProcessorCommon;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class JsonGenerator2 implements InputOperator
{
  private int adsIdx = 0;
  private int eventsIdx = 0;
  private Random random = new Random();
  private long prevTime;
  private long timeDiff = 200;
  private long burst = 50;

  private String pageID = UUID.randomUUID().toString();
  private String userID = UUID.randomUUID().toString();
  private final String[] eventTypes = new String[]{"view", "click", "purchase"};

  private static final transient Logger logger = LoggerFactory.getLogger(JsonGenerator2.class);

  public int getNumCampaigns()
  {
    return numCampaigns;
  }

  public void setNumCampaigns(int numCampaigns)
  {
    this.numCampaigns = numCampaigns;
  }

  public int getNumAdsPerCampaign()
  {
    return numAdsPerCampaign;
  }

  public void setNumAdsPerCampaign(int numAdsPerCampaign)
  {
    this.numAdsPerCampaign = numAdsPerCampaign;
  }

  private int numCampaigns = 100;
  private int numAdsPerCampaign = 1;

  private List<String> ads;
  private Map<String, List<String>> campaigns;
  private Map<String, String> adToCampaingn = new HashMap<>();
  private Map<String, Long> campaignToLastUpdated = new HashMap<>();
  private int counter = 0;

  private transient CampaignProcessorCommon campaignProcessorCommon;

  public JsonGenerator2()
  {

  }

  public void init()
  {
    this.campaigns = generateCampaigns();
    this.ads = flattenCampaigns();
  }

  public Map<String, List<String>> getCampaigns()
  {
    return campaigns;
  }

  /**
   * Generate a single element
   */
  public void generateElement()
  {
     String ad = ads.get(counter++);

     if (counter == ads.size()) counter = 0;

     Long time = System.currentTimeMillis();
     String campaign = adToCampaingn.get(ad);

     campaignProcessorCommon.execute(campaign, time.toString());
  }

    /**
     * Generate a random list of ads and campaigns
     */
  private Map<String, List<String>> generateCampaigns()
  {
    Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
    for (int i = 0; i < numCampaigns; i++) {
      String campaign = UUID.randomUUID().toString();

      ArrayList<String> ads = new ArrayList<>();
      for (int j = 0; j < numAdsPerCampaign; j++) {
        ads.add(UUID.randomUUID().toString());
        adToCampaingn.put(ads.get(ads.size()-1), campaign);
      }

      adsByCampaign.put(campaign, ads);
    }

    return adsByCampaign;
  }

    /**
     * Flatten into just ads
     */
  private List<String> flattenCampaigns()
  {
    // Flatten campaigns into simple list of ads
    List<String> ads = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      for (String ad : entry.getValue()) {
        ads.add(ad);
      }
    }

    return ads;
  }

  @Override
  public void emitTuples()
  {
    for (int index = 0; index < burst; ++index) {
      generateElement();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {

    campaignProcessorCommon = new CampaignProcessorCommon("node35.morado.com");
    campaignProcessorCommon.prepare();

  /*  logger.info("Printing all adIds");

    if (ads == null) return;

    for (int i = 0; i < ads.size(); ++i) {
      logger.info(ads.get(i));
    }

    logger.info("done printing all ad ids"); */
  }

  @Override
  public void teardown()
  {

  }

  public static void latency(long eventTime)
  {
    long time = System.currentTimeMillis();

    long actual =  time - eventTime;
    long yahoo = time - (eventTime/10000) * 10000 - 10000;

    if ( Math.abs(actual - yahoo) >= 2000 ) {
      System.out.println(eventTime + "  " + time + " Actual: " + actual + " yahoo: " + yahoo );
    }
  }

  public long getTimeDiff()
  {
    return timeDiff;
  }

  public void setTimeDiff(long timeDiff)
  {
    this.timeDiff = timeDiff;
  }

  public long getBurst()
  {
    return burst;
  }

  public void setBurst(long burst)
  {
    this.burst = burst;
  }
}

