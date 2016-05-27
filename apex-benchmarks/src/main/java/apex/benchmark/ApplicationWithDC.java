/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = ApplicationWithDC.APP_NAME)
public class ApplicationWithDC extends ApplicationDimensionComputation
{
  public static final String APP_NAME = "ApplicationWithDC";

  protected static final int PARTITION_NUM = 8;
  
  public ApplicationWithDC()
  {
    super(APP_NAME);
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    DefaultOutputPort<DimensionTuple> upstreamOutput = populateUpstreamDAG(dag, configuration);

    //populateHardCodedDimensionsDAG(dag, configuration, generateOperator.outputPort);
    populateDimensionsDAG(dag, configuration, upstreamOutput);
  }

  public DefaultOutputPort<DimensionTuple> populateUpstreamDAG(DAG dag, Configuration configuration)
  {
    EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    TupleToDimensionTupleConverter converter = dag.addOperator("converter", new TupleToDimensionTupleConverter());

    // Connect the Ports in the Operators
    dag.addStream("deserialize", eventGenerator.out, deserializeJSON.input);
    dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("converter", redisJoin.output, converter.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);

    dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(converter.inputPort, Context.PortContext.PARTITION_PARALLEL, true);

    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(PARTITION_NUM));

    return converter.outputPort;
  }
}
