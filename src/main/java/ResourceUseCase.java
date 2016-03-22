import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.03.16
 */
abstract public class ResourceUseCase extends ReadUseCase {
    @Override
    public List<Path> chooseFiles(List<Path> rawFileList) {
        return rawFileList.stream().filter(p -> p.toString().contains("_resource")).collect(Collectors.toList());
    }
}
