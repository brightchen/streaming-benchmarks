package apex.benchmark;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class TupleToDimensionTupleConverter extends BaseOperator
{
  public transient DefaultInputPort<Tuple> inputPort = new DefaultInputPort<Tuple>()
  {
    @Override
    public void process(Tuple tuple)
    {
      processTuple(tuple);
    }
  };

  public final transient DefaultOutputPort<DimensionTuple> outputPort = new DefaultOutputPort<DimensionTuple>();

  public void processTuple(Tuple tuple)
  {
    outputPort.emit(DimensionTuple.fromTuple(tuple));
  }
}
