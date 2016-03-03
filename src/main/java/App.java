import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.log4j.BasicConfigurator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;


/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 02.03.16
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private enum lsbLabels implements Label {
        BIBLIOGRAPHICRESOURCE, PERSON, ORGANISATION, ITEM, LOCALSIGNATURE
    }

    public enum lsbRelations implements RelationshipType {
        CONTRIBUTOR, ITEMOF, SIGNATUREOF
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        GraphDatabaseService graphDb;
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("/tmp/neo4j.data"));

        ArrayList<Path> fileList = traverseRootDir(Paths.get("/data/sbdump/baseLineOutput_flat"));
        LOG.info("{} files to process", fileList.size());
        JsonFactory jsonFactory = new JsonFactory();

        for (Path f : fileList) {
            try (Transaction tx = graphDb.beginTx()) {
                LOG.info("Processing file {}", f.toString());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f.toString()))))) {
                    String line;
                    int counter = 1;
                    String type = "";
                    while ((line = reader.readLine()) != null) {
                        LOG.trace("Processing line {}", counter);
                        HashMap<String, ArrayList<String>> jsonVals;
                        if (counter == 1) {
                            jsonVals = extract(jsonFactory, line);
                            type = jsonVals.get("_type").get(0);
                            LOG.trace("File contains documents of type {}", type);
                        } else if (counter % 2 == 0) {
                            switch (type) {
                                case "bibliographicResource":
                                    jsonVals = extract(jsonFactory, line);
                                    Node brNode = merge(graphDb, lsbLabels.BIBLIOGRAPHICRESOURCE, jsonVals.get("@id").get(0), true);
                                    for (String v : jsonVals.get("dct:contributor")) {
                                        Node brRef;
                                        if (v.contains("person")) {
                                            brRef = merge(graphDb, lsbLabels.PERSON, v, false);
                                        } else {
                                            brRef = merge(graphDb, lsbLabels.ORGANISATION, v, false);
                                        }
                                        brRef.createRelationshipTo(brNode, lsbRelations.CONTRIBUTOR);
                                    }
                                    break;
                                case "person":
                                    jsonVals = extract(jsonFactory, line);
                                    merge(graphDb, lsbLabels.PERSON, jsonVals.get("@id").get(0), false);
                                    break;
                                case "organisation":
                                    jsonVals = extract(jsonFactory, line);
                                    merge(graphDb, lsbLabels.ORGANISATION, jsonVals.get("@id").get(0), false);
                                    break;
                                case "item":
                                    jsonVals = extract(jsonFactory, line);
                                    Node iNode = merge(graphDb, lsbLabels.ITEM, jsonVals.get("@id").get(0), false);
                                    Node iRef = merge(graphDb, lsbLabels.BIBLIOGRAPHICRESOURCE, jsonVals.get("bf:holdingFor").get(0), true);
                                    iNode.createRelationshipTo(iRef, lsbRelations.ITEMOF);
                                    break;
                                case "document":
                                    jsonVals = extract(jsonFactory, line);
                                    for (String l : jsonVals.get("bf:local")) {
                                        Node lNode = merge(graphDb, lsbLabels.LOCALSIGNATURE, l, false);
                                        Node lRef = merge(graphDb, lsbLabels.BIBLIOGRAPHICRESOURCE, jsonVals.get("@id").get(0).replace("/about", ""), true);
                                        lNode.createRelationshipTo(lRef, lsbRelations.SIGNATUREOF);
                                    }
                                    break;
                            }
                        }
                        tx.success();
                        counter += 1;
                    }
                } catch (IOException e) {
                    LOG.error("IO exception: {}", e);
                }
                tx.close();
            }

        }
        registerShutdownHook(graphDb);
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    protected static HashMap<String, ArrayList<String>> extract(final JsonFactory jsonFactory, final String jsonLine) throws IOException {
        HashMap<String, ArrayList<String>> hm = new HashMap<>();
        JsonParser jp = jsonFactory.createParser(jsonLine);
        boolean inArray = false;
        ArrayList<String> valueArray = new ArrayList<>();
        while (jp.nextToken() != null) {
            String fieldName = jp.getCurrentName();
            if (jp.getCurrentToken().equals(JsonToken.START_ARRAY)) {
                inArray = true;
                jp.nextToken();
                valueArray.add(jp.getValueAsString());
            } else if (inArray && !(jp.getCurrentToken().equals(JsonToken.END_ARRAY))) {
                valueArray.add(jp.getValueAsString());
            } else {
                if (inArray) {
                    inArray = false;
                } else {
                    valueArray.add(jp.getValueAsString());
                }
                jp.nextToken();
                if (fieldName != null && (fieldName.equals("@id")
                        || fieldName.equals("dct:contributor")
                        || fieldName.equals("bf:holdingFor")
                        || fieldName.equals("bf:local")
                        || fieldName.equals("_type"))) {
                    hm.put(fieldName, (ArrayList<String>) valueArray.clone());
                }
                valueArray.clear();
            }
        }
        return hm;

    }

    protected static Node merge(GraphDatabaseService g, Label l, String v, boolean a) {
        Node n;
        if ((n = g.findNode(l, "name", v)) == null) {
            LOG.trace("Node with label {} and property name={} not found, thus creating it.", l.toString(), v);
            n = g.createNode(l);
            n.setProperty("name", v);
            if (a)
                n.setProperty("active", "true");
        } else {
            LOG.trace("Node with label {} and property name={} already exists, thus leaving as is.", l.toString(), v);
        }
        return n;
    }

    public static ArrayList<Path> traverseRootDir(final Path rootdir) {
        ArrayList<Path> fileList = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootdir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    fileList.addAll(traverseRootDir(entry));
                } else {
                    if (entry.toString().endsWith("jsonld.gz")) {
                        fileList.add(entry);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("IO exception: {}", e);
        }
        return fileList;
    }

}
