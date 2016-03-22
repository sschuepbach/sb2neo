import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.nio.file.Path;
import java.util.List;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.03.16
 */
abstract public class UseCase {

    NeoConnect nc;

    public void start(List<Path> rawFileList, String pathToDb, int batchSize) {
    }

    public void start(String pathToDb) {
    }

    enum lsbLabels implements Label {
        BIBLIOGRAPHICRESOURCE, PERSON, ORGANISATION, ITEM, LOCALSIGNATURE
    }

    enum lsbRelations implements RelationshipType {
        CONTRIBUTOR, ITEMOF, SIGNATUREOF
    }

}
