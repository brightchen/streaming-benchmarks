package apex.benchmark;

/**
 * Created by sandesh on 7/11/16.
 */

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.io.IOException;

public class SampleTest {
@Test
public void testApplicationStringCodec() throws IOException, Exception {
    try {

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new ApplicationWithGenerator(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(); // runs for 10 seconds and quits
      } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
      }
    }
}
