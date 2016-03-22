import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;


/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 02.03.16
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) {

        String inputPath = "/home/seb/tmp/baseLineOutput_flat";
        //String pathToDb = "/home/seb/neo4j.data";
        String pathToDb = "/data/neo4j/databases/neo4j-data";
        int batchSize = 100000;

        // Set up Logging framework
        BasicConfigurator.configure();

        ArrayList<Path> fileList = traverseRootDir(Paths.get(inputPath));
        LOG.info("{} files to process", fileList.size());

        UseCase uc;

        switch(args[0]) {
            case "loadResourceWithIndices":
                uc = new LoadResourceWithIndicesUseCase();
                uc.start(fileList, pathToDb, batchSize);
                break;
            case "loadResourceWithoutIndices":
                uc = new LoadResourceWithoutIndicesUseCase();
                uc.start(fileList, pathToDb, batchSize);
                break;
            case "mergeResourceWithIndices":
                uc = new MergeResourceWithIndicesUseCase();
                uc.start(fileList, pathToDb, batchSize);
                break;
            case "mergeResourceWithoutIndices":
                uc = new MergeResourceWithoutIndicesUseCase();
                uc.start(fileList, pathToDb, batchSize);
                break;
            case "setSchemataForResources":
                uc = new SetSchemataForResources();
                uc.start(pathToDb);
                break;
            case "findResources":
                uc = new FindResourcesUseCase();
                uc.start(fileList, pathToDb, batchSize);
                break;
            default:
                LOG.error("{} is not a valid argument", args[0]);
                System.exit(1);
        }

/*                        switch (type) {
                            case "person":
                                jsonVals = extract(jsonFactory, line);
                                HashMap<String, String> p2 = new HashMap<>();
                                p2.put("name", jsonVals.get("@id").get(0));
                                nc.addNode(lsbLabels.PERSON, p2);
                                break;
                            case "organisation":
                                jsonVals = extract(jsonFactory, line);
                                HashMap<String, String> p3 = new HashMap<>();
                                p3.put("name", jsonVals.get("@id").get(0));
                                nc.addNode(lsbLabels.ORGANISATION, p3);
                                break;
                            case "item":
                                jsonVals = extract(jsonFactory, line);
                                HashMap<String, String> p4 = new HashMap<>();
                                p4.put("name", jsonVals.get("@id").get(0));
                                nc.addNode(lsbLabels.ITEM, p4);
                                    *//*Node iNode = merge(graphDb, lsbLabels.ITEM, jsonVals.get("@id").get(0), false);
                                    Node iRef = merge(graphDb, lsbLabels.BIBLIOGRAPHICRESOURCE, jsonVals.get("bf:holdingFor").get(0), true);
                                    iNode.createRelationshipTo(iRef, lsbRelations.ITEMOF);*//*
                                break;
                            case "document":
                                jsonVals = extract(jsonFactory, line);
                                for (String l : jsonVals.get("bf:local")) {
                                    HashMap<String, String> p = new HashMap<>();
                                    p.put("name", l);
                                    nc.addNode(lsbLabels.LOCALSIGNATURE, p);
                                        *//*Node lNode = merge(graphDb, lsbLabels.LOCALSIGNATURE, l, false);
                                        Node lRef = merge(graphDb, lsbLabels.BIBLIOGRAPHICRESOURCE, jsonVals.get("@id").get(0).replace("/about", ""), true);
                                        lNode.createRelationshipTo(lRef, lsbRelations.SIGNATUREOF);*//*
                                }
                                break;
                        }*/
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
