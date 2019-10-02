package amanda.edms;

import com.opentext.ecm.api.OTAuthentication;
import com.opentext.livelink.service.core.Authentication;
import com.opentext.livelink.service.core.Authentication_Service;
import com.opentext.livelink.service.core.ContentService;
import com.opentext.livelink.service.core.ContentService_Service;
import com.opentext.livelink.service.core.FileAtts;
import com.opentext.livelink.service.core.StringValue;
import com.opentext.livelink.service.docman.AttributeGroup;
import com.opentext.livelink.service.docman.DocumentManagement;
import com.opentext.livelink.service.docman.DocumentManagement_Service;
import com.sun.xml.internal.ws.api.message.Header;
import com.sun.xml.internal.ws.api.message.Headers;
import com.sun.xml.internal.ws.developer.WSBindingProvider;

import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPHeaderElement;
import javax.xml.ws.soap.MTOMFeature;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class that creates JAX-WS services based on edms.properties setting
 */
public class OpenTextServices {

    // Namespaces for the SOAP headers
    private static final String CORE_NAMESPACE = "urn:Core.service.livelink.opentext.com";

    // We need to manually set the SOAP header to include the authentication token
    // The namespace of the OTAuthentication object
    private static final String ECM_API_NAMESPACE = "urn:api.ecm.opentext.com";

    private static ContentService_Service contentService_service;

    private static DocumentManagement_Service documentManagement_service;

    private static Authentication_Service authentication_service;

    private OpenTextServices() {
        throw new Error("Contains only static methods");
    }

    public static DocumentManagement getDocService(String authToken) throws SOAPException, MalformedURLException {
        if (documentManagement_service == null) {
            URL url = new URL(EdmsProperties.getServerDocumentURL());
            documentManagement_service = new DocumentManagement_Service(url);
        }
        DocumentManagement docManClient = documentManagement_service.getBasicHttpBindingDocumentManagement();

        // Create the OTAuthentication object and set the authentication token
        OTAuthentication otAuth = new OTAuthentication();
        otAuth.setAuthenticationToken(authToken);

        {
            // Create a SOAP header
            SOAPHeader header = MessageFactory.newInstance().createMessage().getSOAPPart().getEnvelope().getHeader();

            // Add the OTAuthentication SOAP header element
            SOAPHeaderElement otAuthElement = header.addHeaderElement(new QName(ECM_API_NAMESPACE, "OTAuthentication"));

            // Add the AuthenticationToken SOAP element
            SOAPElement authTokenElement = otAuthElement.addChildElement(new QName(ECM_API_NAMESPACE, "AuthenticationToken"));
            authTokenElement.addTextNode(otAuth.getAuthenticationToken());

            // Set the SOAP header on the docManClient
            ((WSBindingProvider) docManClient).setOutboundHeaders(Headers.create(otAuthElement));
        }
        return docManClient;
    }

    public static ContentService getContentService(String authToken, String contentId, FileAtts fileAtts) throws SOAPException, MalformedURLException {
        // Create the ContentService client
        // NOTE: ContentService is the only service that requires MTOM support
        if (contentService_service == null) {
            URL url = new URL(EdmsProperties.getServerContentURL());
            contentService_service = new ContentService_Service(url);
        }
        ContentService contentServiceClient = contentService_service.getBasicHttpBindingContentService(new MTOMFeature());

        // Create the OTAuthentication object and set the authentication token
        OTAuthentication otAuth = new OTAuthentication();
        otAuth.setAuthenticationToken(authToken);

        {
            // Create a SOAP header
            SOAPHeader header = MessageFactory.newInstance().createMessage().getSOAPPart().getEnvelope().getHeader();

            // Add the OTAuthentication SOAP header element
            SOAPHeaderElement otAuthElement = header.addHeaderElement(new QName(ECM_API_NAMESPACE, "OTAuthentication"));

            // Add the AuthenticationToken
            SOAPElement authTokenElement = otAuthElement.addChildElement(new QName(ECM_API_NAMESPACE, "AuthenticationToken"));
            authTokenElement.addTextNode(otAuth.getAuthenticationToken());

            // Add the ContextID SOAP header element
            SOAPHeaderElement contextIDElement = header.addHeaderElement(new QName(CORE_NAMESPACE, "contextID"));
            contextIDElement.addTextNode(contentId);

            // Set the headers on the binding provider
            List<Header> headers = new ArrayList<Header>();
            headers.add(Headers.create(otAuthElement));
            headers.add(Headers.create(contextIDElement));
            if (fileAtts != null) {
                // Add the FileAtts SOAP header element
                SOAPHeaderElement fileAttsElement = header.addHeaderElement(new QName(OpenTextServices.CORE_NAMESPACE, "fileAtts"));

                // Add the CreatedDate element
                SOAPElement createdDateElement = fileAttsElement.addChildElement(new QName(OpenTextServices.CORE_NAMESPACE, "CreatedDate"));
                createdDateElement.addTextNode(fileAtts.getCreatedDate().toString());

                // Add the ModifiedDate element
                SOAPElement modifiedDateElement = fileAttsElement.addChildElement(new QName(OpenTextServices.CORE_NAMESPACE, "ModifiedDate"));
                modifiedDateElement.addTextNode(fileAtts.getModifiedDate().toString());

                // Add the FileSize element
                SOAPElement fileSizeElement = fileAttsElement.addChildElement(new QName(OpenTextServices.CORE_NAMESPACE, "FileSize"));
                fileSizeElement.addTextNode(fileAtts.getFileSize().toString());

                // Add the FileName element
                SOAPElement fileNameElement = fileAttsElement.addChildElement(new QName(OpenTextServices.CORE_NAMESPACE, "FileName"));
                fileNameElement.addTextNode(fileAtts.getFileName());

                headers.add(Headers.create(fileAttsElement));
            }
            ((WSBindingProvider) contentServiceClient).setOutboundHeaders(headers);
        }
        return contentServiceClient;
    }

    public static Authentication getAuthService() throws MalformedURLException {
        if (authentication_service == null) {
            URL url = new URL(EdmsProperties.getServerAuthURL());
            authentication_service = new Authentication_Service(url);
        }
        // Create the Authentication service client
        Authentication authClient = authentication_service.getBasicHttpBindingAuthentication();
        return authClient;
    }

    public static AttributeGroup buildDocumentCategory(String authToken, String[] docMetaData) throws MalformedURLException, SOAPException {
        DocumentManagement docService = getDocService(authToken);
        AttributeGroup documentInfoCategoryTemplate = docService.getCategoryTemplate(EdmsProperties.getDocumentInfoId());

        // Document Source
        StringValue DocumentSourceByValue = (StringValue) documentInfoCategoryTemplate.getValues().get(0);
        DocumentSourceByValue.getValues().clear();
        DocumentSourceByValue.getValues().add(docMetaData[1]);

        // Document Description
        StringValue DescriptionByValue = (StringValue) documentInfoCategoryTemplate.getValues().get(1);
        DescriptionByValue.getValues().clear();
        if (docMetaData[2].length() > 80) {
            DescriptionByValue.getValues().add(docMetaData[2].substring(0, 80)); // Truncate to 80 Characters
        } else {
            DescriptionByValue.getValues().add(docMetaData[2]);
        }

        // Attachment Description
        StringValue AttachmentDescByValue = (StringValue) documentInfoCategoryTemplate.getValues().get(2);
        AttachmentDescByValue.getValues().clear();
        AttachmentDescByValue.getValues().add(docMetaData[3]);
        return documentInfoCategoryTemplate;
    }
}
