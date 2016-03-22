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
abstract public class LoadResourceUseCase extends ResourceUseCase{

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    @Override
    public void process(String line) {
        try {
            HashMap<String, ArrayList<String>> jsonVals = extract(jsonFactory, line);
            HashMap<String, String> p1 = new HashMap<>();
            p1.put("name", jsonVals.get("@id").get(0));
            nc.addNode(lsbLabels.BIBLIOGRAPHICRESOURCE, p1);
        } catch (IOException e) {
            LOG.error("IO exception: {}", e);
        }
    }
}
