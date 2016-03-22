
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.03.16
 */
public class FindResourcesUseCase extends ResourceUseCase{

    private final static Logger LOG = LoggerFactory.getLogger(App.class);
    StopWatch stopWatch = new StopWatch();


    @Override
    void prepareDb() {

    }

    @Override
    void process(String line) {
        HashMap<String, ArrayList<String>> jsonVals;
        try {
            if (!stopWatch.hasStarted()) stopWatch.start();
            jsonVals = extract(jsonFactory, line);
            nc.findNode(lsbLabels.BIBLIOGRAPHICRESOURCE, "name", jsonVals.get("@id").get(0));
            long lapTime = stopWatch.lapTime();
            LOG.debug("Finding BIBLIOGRAPHICRESOURCE node {} took {} milliseconds", jsonVals.get("@id"), lapTime);
        } catch (IOException e) {
            LOG.error("IO exception: {}", e);
        }

    }
}
