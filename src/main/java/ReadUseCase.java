import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 21.03.16
 */
abstract public class ReadUseCase extends UseCase {

    JsonFactory jsonFactory = new JsonFactory();

    abstract List<Path> chooseFiles(List<Path> rawFileList);

    abstract void prepareDb();

    @Override
    public void start(List<Path> rawFileList, String pathToDb, int batchSize) {
        List<Path> pathList = chooseFiles(rawFileList);
        nc = new NeoConnect(pathToDb, batchSize);
        prepareDb();
        pathList.forEach(this::readFile);
    }

    void readFile(Path filePath) {
        int counter = 1;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath.toString()))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (counter % 2 == 0) {
                    process(line);
                }
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract void process(String line);

    static HashMap<String, ArrayList<String>> extract(final JsonFactory jsonFactory, final String jsonLine) throws IOException {
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


}
