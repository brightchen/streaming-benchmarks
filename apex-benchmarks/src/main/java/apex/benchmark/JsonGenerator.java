/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.*;

import com.datatorrent.api.Context;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonGenerator implements InputOperator
{

  public final transient DefaultOutputPort<JSONObject> out = new DefaultOutputPort<JSONObject>();

  private int adsIdx = 0;
  private int eventsIdx = 0;
  private Random random = new Random();
  private long prevTime;
  private long timeDiff = 200;
  private long burst = 50;

  private String pageID = UUID.randomUUID().toString();
  private String userID = UUID.randomUUID().toString();
  private final String[] eventTypes = new String[]{"view", "click", "purchase"};

  private static final transient Logger logger = LoggerFactory.getLogger(JsonGenerator.class);

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

  private Map<Long, Map<String,List<Long> > > mappings = new HashMap<>();

  public JsonGenerator()
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
  public JSONObject generateElement()
  {
    JSONObject jsonObject = new JSONObject();
    try  {

      jsonObject.put("user_id", userID);
      jsonObject.put("page_id", pageID);

      String ad = ads.get(counter++);
      jsonObject.put("ad_id", ad);

      if (counter == ads.size()) counter = 0;

      jsonObject.put("ad_type", "banner78");
      jsonObject.put("event_type", eventTypes[random.nextInt(eventTypes.length)]);

      long time = System.currentTimeMillis();

      /*

      String campaign = adToCampaingn.get(ad);

      long timeBucket = ( time / 10000 ) * 10000;

      if ( mappings.containsKey(timeBucket)) {

        Map<String, List<Long>> campaigns = mappings.get(timeBucket);

        if ( campaigns.containsKey(campaign)) {

          campaigns.get(campaign).add(1,time);
        } else {
          List<Long> times = new ArrayList<>();
          times.add(time);
          times.add(time);

          campaigns.put(campaign, times);
        }

      } else {

        for ( Map.Entry<Long, Map<String,List<Long>>> items: mappings.entrySet() ) {

          logger.info("TimeBucket {} ",  items.getKey());

          for ( Map.Entry<String,List<Long>> valueItems: items.getValue().entrySet()) {

            logger.info("Campaign {}, StartTime {}, EndTime {}", valueItems.getKey(), valueItems.getValue().get(0), valueItems.getValue().get(1));
          }
        }

       mappings.clear();

        List<Long> times = new ArrayList<>();
        times.add(time);
        times.add(time);

        mappings.put(timeBucket, new HashMap<String, List<Long>>());
        mappings.get(timeBucket).put(campaign, times);
      } */

      jsonObject.put("event_time", time);
      jsonObject.put("ip_address", "1.2.3.4");

    }  catch ( JSONException json) {
        throw new RuntimeException(json);
    }

    return jsonObject;
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
        adToCampaingn.put(ads.get(ads.size()-1),campaign);
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

      out.emit(generateElement());
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

