package amanda.edms;

import com.opentext.livelink.service.core.ContentService;
import com.opentext.livelink.service.docman.DocumentManagement;
import com.opentext.livelink.service.docman.Node;
import com.sun.xml.internal.org.jvnet.staxex.StreamingDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.soap.SOAPException;
import java.io.File;
import java.io.IOException;

/**
 * Service class to retrieve attachments from OpenText server
 */
public class OTGetAttachmentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OTGetAttachmentService.class);

    private OTGetAttachmentService() {
        throw new Error("Contains only static methods");
    }

    public static OTFileContent getContent(String authToken, Long documentId) throws SOAPException, IOException {
        // Create the DocumentManagement service client
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);

        Node docNode = docManClient.getNode(documentId);
        String fileName = docNode.getName();
        String displayType = docNode.getDisplayType();
        LOGGER.debug("OT FileName: {}", fileName);
        LOGGER.debug("DisplayType: {}", displayType);

        String contentId = docManClient.getVersionContentsContext(documentId, 0);
        LOGGER.debug("OT Content ID: {}", contentId);

        // Create the ContentService client
        // NOTE: ContentService is the only service that requires MTOM support
        ContentService contentServiceClient = OpenTextServices.getContentService(authToken, contentId, null);

        StreamingDataHandler downloadStream = (StreamingDataHandler) contentServiceClient.downloadContent(contentId);
        String tempFileName = "open-text-" + System.currentTimeMillis();
        File tempFile = File.createTempFile(tempFileName, "download");
        downloadStream.moveTo(tempFile);
        downloadStream.close();

        OTFileContent fileContent = new OTFileContent();
        fileContent.setFileName(fileName);
        fileContent.setContent(new DeleteOnCloseFileInputStream(tempFile));
        return fileContent;
    }
}
