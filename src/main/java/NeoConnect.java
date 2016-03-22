import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;


/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 17.03.16
 */
public class NeoConnect {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    GraphDatabaseService graphDb;
    int batchSize;
    private int counter;
    private Transaction tx;
    StopWatch stopWatch = new StopWatch();

    NeoConnect(String pathToDb, int batchSize) {
        this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(pathToDb));
        this.batchSize = batchSize;
        tx = graphDb.beginTx();
    }

    public void setSchema(Label l, String p) {
        LOG.info("Setting index on property {} of label {}.", p, l.toString());
        graphDb.schema().indexFor(l).on(p);
        tx.success();
        commit();
    }

    public Node addNode(Label l) {
        if (!stopWatch.hasStarted()) {
            stopWatch.start();
        }
        Node n = graphDb.createNode();
        if (count()) {
            commit();
        }
        tx.success();
        return n;
    }

    public Node addNode(Label l, HashMap<String, String> p) {
        if (!stopWatch.hasStarted()) {
            stopWatch.start();
        }
        Node n = graphDb.createNode(l);
        Iterator it = p.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry) it.next();
            n.setProperty((String) pair.getKey(), pair.getValue());
            it.remove();
        }
        if (count()) {
            commit();
        }
        tx.success();
        return n;
    }

    public Node findNode(Label l, String pK, String pV) {
        return graphDb.findNode(l, pK, pV);
    }

    public Relationship addRelationship(Node n1, Node n2, RelationshipType l) {
        return n1.createRelationshipTo(n2, l);
    }


    public void commit() {
        tx.close();
        long lapTime = stopWatch.lapTime();
        LOG.info("{} milliseconds have passed since last commit.", lapTime);
        tx = graphDb.beginTx();
    }

    private boolean count() {
        if (counter < batchSize) {
            counter++;
            return false;
        } else {
            counter = 0;
            return true;
        }
    }

    public void disconnect() {
        tx.close();
        stopWatch.reset();
        registerShutdownHook(graphDb);
    }

    public Node merge(Label l, String pK, String pV) {
        Node n;
        if ((n = graphDb.findNode(l, pK, pV)) == null) {
            LOG.trace("Node with label {} and property {}:{} not found, thus creating it.", l.toString(), pK, pV);
            HashMap<String, String> p = new HashMap<>();
            p.put(pK, pV);
            addNode(l, p);
        } else {
            LOG.trace("Node with label {} and property {}:{} already exists, thus leaving as is.", l.toString(), pK, pV);
        }
        return n;
    }

    protected static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

}
