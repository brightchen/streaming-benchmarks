/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;

import com.datatorrent.lib.appdata.schemas.TimeBucket;

public class DimensionTuple implements Serializable
{
  private static final long serialVersionUID = -4382320567154145354L;
  public static final String ADID = "adId";
  public static final String CAMPAIGNID = "campaignId";
  public static final String EVENTTIME = "eventTime";
  public static final String CLICKS = "clicks";

  public String adId;
  public String campaignId;

  public long eventTime;
  public long clicks;

  public static DimensionTuple fromTuple(Tuple tuple)
  {
    return new DimensionTuple(tuple);
  }
  
  public DimensionTuple()
  {
  }

  public DimensionTuple(Tuple tuple)
  {
    this(tuple.adId, tuple.campaignId, Long.valueOf(tuple.event_time), Long.valueOf(tuple.clicks));
  }

  /**
   * following code are for Hard Coded Dimension Computation only
   */

  public DimensionTuple(String adId, String campaignId, long eventTime, long clicks)
  {
    this.adId = adId;
    this.campaignId = campaignId;
    this.eventTime = eventTime;
    this.clicks = clicks;
  }

  public String getAdId()
  {
    return adId;
  }

  public void setAdId(String adId)
  {
    this.adId = adId;
  }

  public String getCampaignId()
  {
    return campaignId;
  }

  public void setCampaignId(String campaignId)
  {
    this.campaignId = campaignId;
  }

  public long getEventTime()
  {
    return eventTime;
  }

  public void setEventTime(long eventTime)
  {
    this.eventTime = eventTime;
  }

  public long getTime()
  {
    return eventTime;
  }
  public void setTime(long time)
  {
    this.setEventTime(time);
  }
  
  public long getClicks()
  {
    return clicks;
  }

  public void setClicks(long clicks)
  {
    this.clicks = clicks;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 71 * hash + this.adId.hashCode();
    hash = 71 * hash + this.campaignId.hashCode();
    hash = 71 * hash + (int)(long)this.eventTime;
    hash = 71 * hash + (int)this.clicks;

    return hash;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof DimensionTuple)) {
      return false;
    }

    DimensionTuple tuple = (DimensionTuple)o;

    return this.adId.equals(tuple.adId) && this.campaignId.equals(tuple.campaignId) && this.eventTime == tuple.eventTime
        && this.clicks == tuple.clicks;
  }
  
  public static class TupleAggregateEvent extends DimensionTuple implements AggregateEvent
  {
    private static final long serialVersionUID = 1L;
    int aggregatorIndex;
    public int timeBucket;
    private int dimensionsDescriptorID;

    public TupleAggregateEvent()
    {
      //Used for kryo serialization
    }

    public TupleAggregateEvent(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    @Override
    public int getAggregatorIndex()
    {
      return aggregatorIndex;
    }

    public void setAggregatorIndex(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    /**
     * @return the dimensionsDescriptorID
     */
    public int getDimensionsDescriptorID()
    {
      return dimensionsDescriptorID;
    }

    /**
     * @param dimensionsDescriptorID
     *          the dimensionsDescriptorID to set
     */
    public void setDimensionsDescriptorID(int dimensionsDescriptorID)
    {
      this.dimensionsDescriptorID = dimensionsDescriptorID;
    }

    @Override
    public int hashCode()
    {
      int hash = 5;
      hash = 71 * hash + this.adId.hashCode();
      hash = 71 * hash + this.campaignId.hashCode();
      hash = 71 * hash + (int)(long)this.eventTime;
      hash = 71 * hash + (int)this.clicks;
      hash = 71 * hash + this.timeBucket;

      return hash;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || !(o instanceof TupleAggregateEvent)) {
        return false;
      }

      TupleAggregateEvent aae = (TupleAggregateEvent)o;

      return this.adId.equals(aae.adId) && this.campaignId.equals(aae.campaignId) && this.eventTime == aae.eventTime
          && this.clicks == aae.clicks && this.timeBucket == aae.timeBucket;
    }
  }

  
  public static class TupleAggregator implements Aggregator<DimensionTuple, TupleAggregateEvent>
  {
    String dimension;
    TimeBucket timeBucket;
    int timeBucketInt;
    TimeUnit time;
    
    boolean hasAdId;
    boolean hasCompainId;
    int dimensionsDescriptorID;

    public void init(String dimension, int dimensionsDescriptorID)
    {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          time = TimeUnit.valueOf(keyval[1]);
          timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(time);
          timeBucketInt = timeBucket.ordinal();
          time = timeBucket.getTimeUnit();
        }
        else if (key.equals("adId")) {
          hasAdId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("campaignId")) {
          hasCompainId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }

        else {
          throw new IllegalArgumentException("Unknown attribute '" + attribute + "' specified as part of dimension!");
        }
      }

      this.dimensionsDescriptorID = dimensionsDescriptorID;
      this.dimension = dimension;
    }

    /**
     * Dimension specification for display in operator properties.
     * @return The dimension.
     */
    public String getDimension()
    {
      return dimension;
    }

    @Override
    public String toString()
    {
      return dimension;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 37 * hash + (this.time != null ? this.time.hashCode() : 0);
      hash = 37 * hash + (this.hasAdId ? 1 : 0);
      hash = 37 * hash + (this.hasCompainId ? 1 : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final TupleAggregator other = (TupleAggregator) obj;
      if (this.time != other.time) {
        return false;
      }
      if (this.hasAdId != other.hasAdId) {
        return false;
      }
      if (this.hasCompainId != other.hasCompainId) {
        return false;
      }
      return true;
    }

    @Override
    public TupleAggregateEvent getGroup(DimensionTuple src, int aggregatorIndex)
    {
      TupleAggregateEvent event = new TupleAggregateEvent(aggregatorIndex);
      event.eventTime = timeBucket.roundDown(src.eventTime);
      event.timeBucket = timeBucketInt;

      event.adId = src.adId;
      event.campaignId = src.campaignId;

      event.aggregatorIndex = aggregatorIndex;
      event.dimensionsDescriptorID = dimensionsDescriptorID;

      return event;
    }

    @Override
    public void aggregate(TupleAggregateEvent dest, DimensionTuple src)
    {
      dest.clicks += src.clicks;
    }

    @Override
    public void aggregate(TupleAggregateEvent dest, TupleAggregateEvent src)
    {
      dest.clicks += src.clicks;
    }

    @Override
    public int hashCode(DimensionTuple event)
    {
      int hash = 5;
      hash = 71 * hash + event.adId.hashCode();
      hash = 71 * hash + event.campaignId.hashCode();
      
      long ltime = time.convert(event.eventTime, TimeUnit.MILLISECONDS);
      hash = 71 * hash + (int) (ltime ^ (ltime >>> 32));
          
      return hash;
    }

    @Override
    public boolean equals(DimensionTuple event1, DimensionTuple event2)
    {
      if (event1 == event2) {
        return true;
      }

      if (event2 == null) {
        return false;
      }

      if (event1.getClass() != event2.getClass()) {
        return false;
      }

      if (!event1.adId.equals(event2.adId)) {
        return false;
      }
      
      if (!event1.campaignId.equals(event2.campaignId)) {
        return false;
      }
      
      if (time != null && time.convert(event1.eventTime, TimeUnit.MILLISECONDS) != time.convert(event2.eventTime, TimeUnit.MILLISECONDS)) {
        return false;
      }
      return true;
    }

  }

}
