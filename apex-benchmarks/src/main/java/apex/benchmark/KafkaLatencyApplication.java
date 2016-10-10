package apex.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.kafka.AbstractKafkaInputOperator.PartitionStrategy;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.stream.DevNull;

@ApplicationAnnotation(name="KafkaLatencyApplication")
public class KafkaLatencyApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create operators for each step
    // settings are applied by the platform using the config file.
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaLatencyOperator());
    ((SimpleKafkaConsumer)(kafkaInput.getConsumer())).setTimeout(30000);
    ((SimpleKafkaConsumer)(kafkaInput.getConsumer())).setBufferSize(64*1024);
    
    kafkaInput.setStrategy(PartitionStrategy.ONE_TO_ONE.name());
    
    DevNull<String> devNull = dag.addOperator("null", new DevNull<String>());
    dag.addStream("discard", kafkaInput.outputPort, devNull.data).setLocality(DAG.Locality.THREAD_LOCAL);
    
    dag.setInputPortAttribute(devNull.data, Context.PortContext.PARTITION_PARALLEL, true);
  }
}