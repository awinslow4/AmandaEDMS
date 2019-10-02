package amanda.edms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * The main class which will be used by Groovy script to perform all the operations
 */
public class OpenTextClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTextClient.class);

    private String username;

    private String password;

    public OpenTextClient() {
        this.username = EdmsProperties.getServerUsername();
        this.password = EdmsProperties.getServerPassword();
    }

    public OTFileContent getAttachment(String documentId) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            String authToken = OTAuthService.authenticate(username, password);
            return OTGetAttachmentService.getContent(authToken, Long.valueOf(documentId));
        } finally {
            long endTime = System.currentTimeMillis();
            LOGGER.info("EDMS:: Time taken to complete get attachment: {}", (endTime - startTime));
        }
    }

    /**
     * When documentId is not empty then the existing document needs to be updated
     */
    public String putAttachment(Connection connection, Integer attachmentRSN, OTFileContent fileContent, Long documentId) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            String authToken = OTAuthService.authenticate(username, password);
            return new OTPutAttachmentService(connection, authToken).putAttachment(attachmentRSN, documentId, fileContent);
        } finally {
            long endTime = System.currentTimeMillis();
            LOGGER.info("EDMS:: Time taken to complete put attachment: {}", (endTime - startTime));
        }
    }

    public void deleteAttachment(String documentId) {
        //TODO need to handle the delete attachment
    }
}
