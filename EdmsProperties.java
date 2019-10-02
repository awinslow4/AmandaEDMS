package amanda.edms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Class loads EDMS properties
 */
public class EdmsProperties {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdmsProperties.class);

    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();
        try {
            InputStream inputStream = EdmsProperties.class.getResourceAsStream("/edms.properties");
            PROPERTIES.load(inputStream);
            inputStream.close();
        } catch (Exception exp) {
            LOGGER.error("Error while loading edms.properties file", exp);
        }
    }

    public static String getServerUsername() {
        return PROPERTIES.getProperty("server.username");
    }

    public static String getServerPassword() {
        return PROPERTIES.getProperty("server.password");
    }

    public static String getServerAuthURL() {
        return PROPERTIES.getProperty("server.auth");
    }

    public static String getServerDocumentURL() {
        return PROPERTIES.getProperty("server.doc");
    }

    public static String getServerContentURL() {
        return PROPERTIES.getProperty("server.content");
    }

    public static Long getTaxAccountId() {
        return Long.valueOf(PROPERTIES.getProperty("openText.taxAccountID"));
    }

    public static Long getDocumentInfoId() {
        return Long.valueOf(PROPERTIES.getProperty("openText.documentInfoId"));
    }

    public static Long getPermitId() {
        return Long.valueOf(PROPERTIES.getProperty("openText.permitId"));
    }

    public static Integer getWorkspaceRootId() {
        return Integer.valueOf(PROPERTIES.getProperty("openText.workspaceRootId"));
    }

    public static Integer getCategoryRootId() {
        return Integer.valueOf(PROPERTIES.getProperty("server.categoryRootId"));
    }

    public static List<String> getTaxAccountFolderArray() {
        return Arrays.asList(PROPERTIES.getProperty("openText.taxAccountFolderArray").split("\\|"));
    }

    public static List<String> getNoTaxAccountFolderArray() {
        return Arrays.asList(PROPERTIES.getProperty("openText.noTaxAccountFolderArray").split("\\|"));
    }

    private static String trimToNull(String value) {
        if (value != null && value.trim().length() > 0) {
            return value;
        }
        return null;
    }
}
