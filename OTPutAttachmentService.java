package amanda.edms;

import com.opentext.livelink.service.core.ContentService;
import com.opentext.livelink.service.core.FileAtts;
import com.opentext.livelink.service.core.StringValue;
import com.opentext.livelink.service.docman.AttributeGroup;
import com.opentext.livelink.service.docman.DocumentManagement;
import com.opentext.livelink.service.docman.Metadata;
import com.opentext.livelink.service.docman.Node;
import com.sun.xml.internal.ws.developer.JAXWSProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.soap.SOAPException;
import javax.xml.ws.BindingProvider;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Service class to store attachments to OpenText server
 */
public class OTPutAttachmentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OTPutAttachmentService.class);

    private static final String COMMENTS_DEFAULT = "Uploaded by AMANDA EDMS Adaptor";

    // Note: This will result in the creation of a major version.
    private static final Boolean ADVANCED_VERSION_CONTROL = Boolean.TRUE;

    private final String authToken;

    private final Connection connection;

    private final AmandaMetaDataService metaDataService;

    public OTPutAttachmentService(Connection connection, String authToken) {
        this.connection = connection;
        this.authToken = authToken;
        metaDataService = new AmandaMetaDataService(connection);
    }

    public String putAttachment(Integer attachmentRSN, Long edmsId, OTFileContent fileContent) throws SOAPException, SQLException, DatatypeConfigurationException, MalformedURLException {
        AttachmentMetaData attachmentMetaData = metaDataService.getAttachmentMetaData(attachmentRSN);
        String fileName = attachmentRSN + "_" + fileContent.getFileName();

        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);

        //You can customize parentId through edms.properties file
        Long parentId = getParentId(attachmentRSN);
        if (parentId == null) {
            throw new RuntimeException("Unable to find ParentID for the AttachmentRSN: " + attachmentRSN);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Found ParentID: {} for the AttachmentRSN: {}", parentId, attachmentRSN);
        }
        FolderData folderData = metaDataService.getFolderData(attachmentRSN);
        String[][] permitMetaData = metaDataService.getStandalonePermitData(folderData);
        Integer folderRSN = Integer.valueOf(permitMetaData[1][6]);
        int numberOfPermits = Integer.parseInt(permitMetaData[0][0]);
        List<String> parcelMetaData = metaDataService.getParcelInfo(folderRSN);

        Metadata attachmentData = new Metadata();
        AttributeGroup CTTaxAccount = buildTaxAccountCategory(parcelMetaData.toArray(new String[parcelMetaData.size()]));
        if (CTTaxAccount != null) {
            attachmentData.getAttributeGroups().add(CTTaxAccount);
        }
        AttributeGroup CTPermit = buildPermitCategory(permitMetaData, numberOfPermits);
        if (CTPermit != null) {
            attachmentData.getAttributeGroups().add(CTPermit);
        }
        AttributeGroup documentInfo = buildDocumentCategory(attachmentMetaData);
        attachmentData.getAttributeGroups().add(documentInfo);

        String contextId;
        if (edmsId == null || edmsId == 0) {
            contextId = docManClient.createDocumentContext(parentId, fileName, COMMENTS_DEFAULT, ADVANCED_VERSION_CONTROL, attachmentData);
        } else {
            contextId = docManClient.addMajorVersionContext(edmsId, attachmentData);
        }
        LOGGER.debug("Context created: {}", contextId);

        // Create the FileAtts object to send in the upload call
        FileAtts fileAtts = new FileAtts();
        GregorianCalendar currentTime = new GregorianCalendar();
        currentTime.setTime(new Date());
        fileAtts.setCreatedDate(DatatypeFactory.newInstance().newXMLGregorianCalendar(currentTime));
        fileAtts.setFileName(fileContent.getFileName());
        if (fileContent.getSize() <= 0) {
            throw new RuntimeException("File size is required");
        }
        fileAtts.setFileSize(fileContent.getSize() * 1L);
        fileAtts.setModifiedDate(DatatypeFactory.newInstance().newXMLGregorianCalendar(currentTime));

        // NOTE: ContentService is the only service that requires MTOM support
        ContentService contentServiceClient = OpenTextServices.getContentService(authToken, contextId, fileAtts);
        // The number of bytes to write in each chunk
        final int CHUNK_SIZE = 10240;

        // Enable streaming and use chunked transfer encoding to send the request body to support large files
        ((BindingProvider) contentServiceClient).getRequestContext().put(JAXWSProperties.HTTP_CLIENT_STREAMING_CHUNK_SIZE, CHUNK_SIZE);


        LOGGER.debug("Uploading document...");
        String objectID = contentServiceClient.uploadContent(new DataHandler(new InputStreamDataSource(fileName, fileContent.getContent())));
        LOGGER.debug("SUCCESS!\n");
        LOGGER.debug("New document uploaded with ID = " + objectID);
        // if the EDMS ID already exists then it means that we have uploaded a newer version
        // in that case use the original EDMS ID instead of new version id.
        if (edmsId != null && edmsId > 0) {
            return String.valueOf(edmsId);
        }

        return objectID;
    }

    private AttributeGroup buildDocumentCategory(AttachmentMetaData metaData) throws SOAPException, MalformedURLException {
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);

        //=================================================================================================

        // Create Meta Data for Categories (Document Info, Permit, Tax Account)

        //===============================================================================================================


        // Create the Document Info Category ===========================================================================

        AttributeGroup DocumentInfoCategoryTemplate = null;

        // Call the getCategoryTemplate() method to get the Document Info category template

        LOGGER.debug("Getting Document Info Category Template...");

        // DocumentInfoCategoryTemplate = docManClient.getCategoryTemplate(Globals.DocumentInfo_ID);
        //You can customize category ID through edms.properties file
        DocumentInfoCategoryTemplate = docManClient.getCategoryTemplate(EdmsProperties.getDocumentInfoId());


        // Set the Document Info category values

        // Document Source
        StringValue DocumentSourceByValue = (StringValue) DocumentInfoCategoryTemplate.getValues().get(0);
        DocumentSourceByValue.getValues().clear();
        DocumentSourceByValue.getValues().add(metaData.getSource());

        // Document Description
        StringValue DescriptionByValue = (StringValue) DocumentInfoCategoryTemplate.getValues().get(1);
        DescriptionByValue.getValues().clear();
        if (metaData.getDescription().length() > 80) {
            DescriptionByValue.getValues().add(metaData.getDescription().substring(0, 80)); // Truncate to 80 Characters
        } else {
            DescriptionByValue.getValues().add(metaData.getDescription());
        }

        // Attachment Description
        StringValue AttachmentDescByValue = (StringValue) DocumentInfoCategoryTemplate.getValues().get(2);
        AttachmentDescByValue.getValues().clear();
        AttachmentDescByValue.getValues().add(metaData.getType());

        return DocumentInfoCategoryTemplate;
    }

    private Long getParentId(Integer attachmentRSN) throws SQLException, SOAPException, MalformedURLException {
        FolderData folderData = metaDataService.getFolderData(attachmentRSN);
        if (folderData != null) {
            String[][] permitMetaData = metaDataService.getStandalonePermitData(folderData);
            int numberOfPermits = Integer.parseInt(permitMetaData[0][0]);
            Integer folderRSN = Integer.valueOf(permitMetaData[1][6]);
            String parcelNumber = permitMetaData[1][5];
            String permitNumber = permitMetaData[1][2];
            List<String> parcelMetaData = metaDataService.getParcelInfo(folderRSN);

            String[] docInfo = {"", "AMANDA Folder", "", "AMANDA-PRISM Loader Created Folder"};
            AttributeGroup CTDocInfo = OpenTextServices.buildDocumentCategory(authToken, docInfo);
            AttributeGroup CTPermit = buildPermitCategory(permitMetaData, numberOfPermits);
            if (CTDocInfo == null) {
                return null;
            }

            // Meta Data to Store Document, Permit and Tax Info
            Metadata docmetadata = new Metadata();
            docmetadata.getAttributeGroups().add(CTDocInfo);
            docmetadata.getAttributeGroups().add(CTPermit);

            AttributeGroup CTTaxAccount = buildTaxAccountCategory(parcelMetaData.toArray(new String[parcelMetaData.size()]));
            if (CTTaxAccount == null) {
                //Put into NO Tax account
                return getNoTaxParentId(permitNumber, docmetadata);
            } else {
                docmetadata.getAttributeGroups().add(CTTaxAccount);
                Long parentId = getTaxParentId(parcelNumber, docmetadata);
                return getPermitParentId(permitNumber, docmetadata, parentId);
            }
        }
        throw new RuntimeException("Unable to fetch folder data for the attachment RSN: " + attachmentRSN);
    }

    private Long getPermitParentId(String PermitNumber, Metadata docmetadata, long ParentID) throws MalformedURLException, SOAPException {
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);
        String OTcomment = "Folder Loaded by AMANDA-PRISM-Loader";

        // Now Get the Tax ID if it Exists
        // Find TaxID Folder using the TAX Account Parent folder we just retreived
        Node PIDNode = docManClient.getNodeByName(ParentID, PermitNumber);
        if (PIDNode == null) {
            LOGGER.debug("Permit Folder doesn't Exist...we must now create it");
            LOGGER.debug("Creating Permit Folder");
            PIDNode = docManClient.createFolder(ParentID, PermitNumber, OTcomment, docmetadata);
            return (PIDNode.getID());
        } else {
            LOGGER.debug("Permit Node Found:" + PIDNode.getID());
            return (PIDNode.getID());
        }
    }

    private Long getNoTaxParentId(String PermitNumber, Metadata docmetadata) throws MalformedURLException, SOAPException {
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);
        Node PIDNode = new Node();
        String OTcomment = "Folder Loaded by AMANDA-PRISM-Loader";

        //===================================================================================================================

        // Get the No Tax Account Folder Node ID
        //     Used later to Verify If Parcel/TAX ID already Exists and Add it if it doesn't

        //==================================================================================================================
        PIDNode = docManClient.getNodeByPath(EdmsProperties.getWorkspaceRootId(), EdmsProperties.getNoTaxAccountFolderArray());
        Long ParentID = PIDNode.getID();
        LOGGER.debug("Tax Account Node ID:" + ParentID);

        // Now Get the Tax ID if it Exists
        // Find TaxID Folder using the TAX Account Parent folder we just retreived
        PIDNode = docManClient.getNodeByName(ParentID, PermitNumber);
        if (PIDNode == null) {
            LOGGER.debug("TaxID Folder doesn't Exist...we must now create it");
            LOGGER.debug("Creating Permit Number (Non Tax Account) Folder");
            PIDNode = docManClient.createFolder(ParentID, PermitNumber, OTcomment, docmetadata);
            return (PIDNode.getID());
        } else {
            LOGGER.debug("Tax ID: Node Found" + PIDNode.getID());
            return (PIDNode.getID());
        }
    }

    private Long getTaxParentId(String TaxID, Metadata docmetadata) throws MalformedURLException, SOAPException {
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);
        String OTcomment = "Folder Loaded by AMANDA-PRISM-Loader";
        LOGGER.debug("Getting Tax Parent ID for TaxID:" + TaxID);

        //===================================================================================================================

        // Get the Tax Account Folder Node ID
        //     Used later to Verify If Parcel/TAX ID already Exists and Add it if it doesn't

        //==================================================================================================================
        Node PIDNode = new Node();
        PIDNode = docManClient.getNodeByPath(EdmsProperties.getWorkspaceRootId(), EdmsProperties.getTaxAccountFolderArray());
        Long ParentID = PIDNode.getID();

        // Now Get the Tax ID if it Exists
        // Find TaxID Folder using the TAX Account Parent folder we just retreived
        PIDNode = docManClient.getNodeByName(ParentID, TaxID);
        if (PIDNode != null) {
            LOGGER.debug("Tax ID:" + TaxID + "Open Text Node Found ID:" + PIDNode.getID());
            return (PIDNode.getID());
        }

        LOGGER.debug("Creating New Tax ID Folder with Tax Only Meta Data");
        PIDNode = docManClient.createFolder(ParentID, TaxID, OTcomment, docmetadata);
        LOGGER.debug("New Tax ID Folder Created Tax ID:" + TaxID);
        return (PIDNode.getID());
    }

    private AttributeGroup buildTaxAccountCategory(String parcelData[]) throws MalformedURLException, SOAPException, SQLException {
        DocumentManagement docManClient = OpenTextServices.getDocService(authToken);
        AttributeGroup taxAccountCategoryTemplate = docManClient.getCategoryTemplate(EdmsProperties.getTaxAccountId());
        int Number_Of_Parcels = parcelData.length;
        if (Number_Of_Parcels == 0) {
            return null;
        }

        // Get Primary Property Info
        String[][] TaxMetaData = metaDataService.getTaxMetaDataInfo(parcelData[0]);
        // Set the Tax Account category values
        if (TaxMetaData[0][0] == null || TaxMetaData[0][0].equals("0")) {
            LOGGER.debug("Build Tax Account Category- TaxMetaData Empty! ");
            return null;
        }
        // Tax ID
        StringValue TaxIDByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(0);
        TaxIDByValue.getValues().clear();
        TaxIDByValue.getValues().add(parcelData[0]);

        // Get Alternate Tax ID
        StringValue AltTaxIDByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(1);
        AltTaxIDByValue.getValues().clear();
        AltTaxIDByValue.getValues().add(TaxMetaData[1][1]);

        // Related Tax Account
        StringValue RelatedTaxIDByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(2);
        RelatedTaxIDByValue.getValues().clear();
        for (int i = 0; i < Number_Of_Parcels; i += 1) {
            RelatedTaxIDByValue.getValues().add(parcelData[i]);
        }

        // Property Address
        StringValue PropertyAddressByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(3);
        PropertyAddressByValue.getValues().clear();
        PropertyAddressByValue.getValues().add(TaxMetaData[1][2]);
        String MainAddress = TaxMetaData[1][2];

        // City
        StringValue CityByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(5);
        CityByValue.getValues().clear();
        CityByValue.getValues().add(TaxMetaData[1][3]);

        // Zip
        StringValue ZipByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(6);
        ZipByValue.getValues().clear();
        if (TaxMetaData[1][4].length() > 5) {
            ZipByValue.getValues().add(TaxMetaData[1][4].substring(0, 5));
        } else {
            ZipByValue.getValues().add(TaxMetaData[1][4]);
        }
        // Related Property Address
        StringValue RelatedAddrByValue = (StringValue) taxAccountCategoryTemplate.getValues().get(4);
        RelatedAddrByValue.getValues().clear();
        int Number_Of_RelatedAddr = 0;
        String[] RelatedAddr = new String[200];

        for (int i = 0; i < Number_Of_Parcels; i += 1) {
            // Get Primary Property Info
            TaxMetaData = metaDataService.getTaxMetaDataInfo(parcelData[i]);
            // Set the Tax Account category values
            if (TaxMetaData[0][0] == null || TaxMetaData[0][0].equals("0")) {
                LOGGER.debug("Build Tax Account Category- TaxMetaData Empty! ");
                return (null);
            } else {
                int Number_Of_Property_Addresses = Integer.parseInt(TaxMetaData[0][0]);
                for (int j = 1; j <= Number_Of_Property_Addresses; j += 1) {
                    boolean Address_Found = false;
                    for (int k = 0; k < Number_Of_RelatedAddr; k += 1) {
                        if (RelatedAddr[k] == TaxMetaData[j][2]) {
                            Address_Found = true;
                        }
                    }
                    if (!Address_Found && (MainAddress != TaxMetaData[j][2])) {
                        RelatedAddr[Number_Of_RelatedAddr] = TaxMetaData[j][2];
                        RelatedAddrByValue.getValues().add(TaxMetaData[j][2]);
                        Number_Of_RelatedAddr += 1;
                    }

                }
            }
            if (Number_Of_RelatedAddr == 0) {
                RelatedAddrByValue.getValues().add(""); // In Case there are no Other Addresses
            }
        }
        return taxAccountCategoryTemplate;
    }

    private AttributeGroup buildPermitCategory(String[][] PermitMetaData, int Number_Of_Permits) throws MalformedURLException, SOAPException {
        DocumentManagement docService = OpenTextServices.getDocService(authToken);
        AttributeGroup PermitCategoryTemplate = docService.getCategoryTemplate(EdmsProperties.getPermitId());

        // Primary Permit Type Value
        StringValue PermitTypeByValue = (StringValue) PermitCategoryTemplate.getValues().get(0);
        PermitTypeByValue.getValues().clear();
        PermitTypeByValue.getValues().add(PermitMetaData[1][0]);

        // Primary Permit Type Description Value
        StringValue PermitDescByValue = (StringValue) PermitCategoryTemplate.getValues().get(1);
        PermitDescByValue.getValues().clear();
        PermitDescByValue.getValues().add(PermitMetaData[1][1]);

        // Primary Permit  Value
        StringValue PermitByValue = (StringValue) PermitCategoryTemplate.getValues().get(2);
        PermitByValue.getValues().clear();
        PermitByValue.getValues().add(PermitMetaData[1][2]);

        // Folder Name Value
        StringValue FolderNameByValue = (StringValue) PermitCategoryTemplate.getValues().get(3);
        FolderNameByValue.getValues().clear();
        FolderNameByValue.getValues().add(PermitMetaData[1][3]);

        // Related Permit Numbers
        StringValue RelatedPermitByValue = (StringValue) PermitCategoryTemplate.getValues().get(4);
        RelatedPermitByValue.getValues().clear();

        String[] RelatedPermitNumberArray = new String[600];

        if (Number_Of_Permits > 1) {
            int Number_Of_Related_Permits = 0;
            for (int i = 1; i < Number_Of_Permits + 1; i += 1)  // Put all the Stored Related Permits Data into Permit Category
            {
                boolean related_found = false;
                for (int j = 0; j < Number_Of_Related_Permits; j += 1) {
                    if (RelatedPermitNumberArray[j] == PermitMetaData[i][2]) {
                        related_found = true;
                        break;
                    }
                }
                if (!related_found && (PermitMetaData[i][2] != PermitMetaData[1][2])) {
                    RelatedPermitNumberArray[Number_Of_Related_Permits] = PermitMetaData[i][2];
                    RelatedPermitByValue.getValues().add(PermitMetaData[i][2]);

                    Number_Of_Related_Permits += 1;
                }
            }
        } else {
            RelatedPermitNumberArray[0] = "";
        }

        // Permit Location Value
        StringValue PermitLocationByValue = (StringValue) PermitCategoryTemplate.getValues().get(5);
        PermitLocationByValue.getValues().clear();
        PermitLocationByValue.getValues().add(PermitMetaData[1][8]);

        // Permit Row ID
        StringValue PermitRSNByValue = (StringValue) PermitCategoryTemplate.getValues().get(6);
        PermitRSNByValue.getValues().clear();
        PermitRSNByValue.getValues().add(PermitMetaData[1][6]);

        // Application Tracking Number
        StringValue ApplicationTrackingNumByValue = (StringValue) PermitCategoryTemplate.getValues().get(7);
        ApplicationTrackingNumByValue.getValues().clear();
        boolean TrackingNumber_Found = false;
        for (int i = 1; i < Number_Of_Permits + 1; i += 1)  // Put all the Stored Related Permits Data into Permit Category
        {
            if (!PermitMetaData[i][9].contentEquals("")) {
                ApplicationTrackingNumByValue.getValues().add(PermitMetaData[i][9]);
                TrackingNumber_Found = true;
                break;
            }

        }
        if (!TrackingNumber_Found) {
            ApplicationTrackingNumByValue.getValues().add("");
        }

        // Section Township Range
        StringValue SectionTownRangeByValue = (StringValue) PermitCategoryTemplate.getValues().get(8);
        SectionTownRangeByValue.getValues().clear();

        int Number_Of_STR = 0;
        String[] SecTwnRng = new String[10];
        for (int i = 1; i < Number_Of_Permits + 1; i += 1) {
            if (!PermitMetaData[i][10].contentEquals("")) {
                // Check to See if we already Have this TSR
                boolean STR_Found = false;
                for (int j = 0; j < Number_Of_STR; j += 1) {
                    if (PermitMetaData[i][10].equals(SecTwnRng[j])) {
                        STR_Found = true;
                        break;
                    }
                }
                if (!STR_Found && (Number_Of_STR < 10)) // Its a New Value and not Past Max
                {
                    SecTwnRng[Number_Of_STR] = PermitMetaData[i][10];  // Add New STR Value
                    SectionTownRangeByValue.getValues().add(PermitMetaData[i][10]);
                    Number_Of_STR += 1;                                         // Point to Next
                }
            }
        }

        // Store Folder Description - Break Down Into Chunks of 320 Chars if Needed.
        StringValue FolderDescByValue = (StringValue) PermitCategoryTemplate.getValues().get(9);
        FolderDescByValue.getValues().clear();

        int FD_Length = PermitMetaData[1][11].length();
        int Number_Of_FD = 0;
        int MaxLength = 320;
        if (FD_Length < MaxLength) {
            MaxLength = FD_Length;
        }

        while ((FD_Length > 0) && (Number_Of_FD < 25))  // Desc Max Size is 4000. Store in Chunks of 320 Chars until all gone.
        {
            FolderDescByValue.getValues().add(PermitMetaData[1][11].substring(0, MaxLength)); // Store another Chuck of Data

            PermitMetaData[1][11] = PermitMetaData[1][11].substring(MaxLength);
            FD_Length = FD_Length - 320;
            Number_Of_FD += Number_Of_FD;
            if (FD_Length < MaxLength) {
                MaxLength = FD_Length;
            }
        }

        // Store People Contacts
        StringValue ContactByValue = (StringValue) PermitCategoryTemplate.getValues().get(10);
        ContactByValue.getValues().clear();
        String[] Contacts = new String[600];
        // Put Unique Contact Info as Meta Data
        int Number_Of_Contacts = 0;
        for (int i = 1; i < Number_Of_Permits + 1; i += 1) {
            boolean Contact_Found = false;
            for (int j = 0; j < Number_Of_Contacts; j += 1) {

                if (PermitMetaData[i][12].equals(Contacts[j])) {
                    Contact_Found = true;
                    break;
                }
            }
            if ((!Contact_Found) && (Number_Of_Contacts < 600) && (PermitMetaData[i][12].indexOf(':') > 0)) {
                String[] Contact = PermitMetaData[i][12].split(":");
                if (!Contact[1].trim().contentEquals("")) {
                    Contacts[Number_Of_Contacts] = PermitMetaData[i][12]; // Save Contact Info
                    ContactByValue.getValues().add(PermitMetaData[i][12]);
                    Number_Of_Contacts += 1;                              //Point to Next Slot
                }

            }
        }
        if (Number_Of_Contacts == 0) {
            ContactByValue.getValues().add("");                // No Contacts Found
        }

        return PermitCategoryTemplate;
    }
}
