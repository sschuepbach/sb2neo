/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 22.03.16
 */
public class LoadResourceWithIndicesUseCase extends LoadResourceUseCase {

    @Override
    void prepareDb() {
        nc.setSchema(lsbLabels.BIBLIOGRAPHICRESOURCE, "name");
    }

}
