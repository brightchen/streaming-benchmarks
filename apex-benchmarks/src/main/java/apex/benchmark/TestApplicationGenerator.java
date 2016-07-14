package apex.benchmark;

import java.io.IOException;
import java.lang.reflect.Field;

import javax.validation.ConstraintViolationException;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
/**
 * Created by sandesh on 6/29/16.
 */
public class TestApplicationGenerator
{
  @Test
  public void testApplication() throws IOException, Exception {
    try {

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new ApplicationWithGenerator2(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
    }
  }
}
