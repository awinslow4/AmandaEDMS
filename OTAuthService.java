package amanda.edms;

import com.opentext.livelink.service.core.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

/**
 * Service class perform OpenText authentication
 */
public class OTAuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OTAuthService.class);

    private OTAuthService() {
        throw new Error("Contains only static methods");
    }

    public static String authenticate(String username, String password) throws MalformedURLException {
        Authentication authClient = OpenTextServices.getAuthService();
        String authToken = authClient.authenticateUser(username, password);
        LOGGER.debug("OT auth successful: {}", authToken);
        return authToken;
    }

}
