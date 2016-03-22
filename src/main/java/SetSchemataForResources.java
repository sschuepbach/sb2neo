import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.03.16
 */
public class SetSchemataForResources extends UseCase{

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    public void start(String pathToDb){
        nc = new NeoConnect(pathToDb, 0);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        nc.setSchema(lsbLabels.BIBLIOGRAPHICRESOURCE, "name");
        HashMap<String, Long> result = stopWatch.reset();
        LOG.info("Setting the index on property name in label BIBLIOGRAPHICRESOURCE took {} milliseconds.", result.get("total"));
    }

}
