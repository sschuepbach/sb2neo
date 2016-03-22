import org.neo4j.graphdb.Node;
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
abstract public class MergeResourceUseCase extends ResourceUseCase {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    @Override
    public void process(String line) {
        try {
            HashMap<String, ArrayList<String>> jsonVals = extract(jsonFactory, line);
            HashMap<String, String> p1 = new HashMap<>();
            p1.put("name", jsonVals.get("@id").get(0));
            Node brNode = nc.addNode(lsbLabels.BIBLIOGRAPHICRESOURCE, p1);
            if (jsonVals.containsKey("dct:contributor")) {
                for (String v : jsonVals.get("dct:contributor")) {
                    Node brRef;
                    if (v.contains("person")) {
                        brRef = nc.merge(lsbLabels.PERSON, "name", v);
                    } else {
                        brRef = nc.merge(lsbLabels.ORGANISATION, "name", v);
                    }
                    brRef.createRelationshipTo(brNode, lsbRelations.CONTRIBUTOR);
                }
            }
        } catch (IOException e) {
            LOG.error("IO exception: {}", e);
        }
    }
}
