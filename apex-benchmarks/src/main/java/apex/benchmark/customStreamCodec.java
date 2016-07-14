package apex.benchmark;

import com.datatorrent.stram.plan.logical.DefaultKryoStreamCodec;

/**
 * Created by sandesh on 7/7/16.
 */
public class customStreamCodec extends DefaultKryoStreamCodec<Tuple>
{
    @Override
    public int getPartition(Tuple t)
    {
      return t.campaignId.hashCode();
    }
}
