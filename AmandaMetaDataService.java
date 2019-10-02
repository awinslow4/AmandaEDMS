package amanda.edms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The class fetches MetaData from AMANDA database.
 */
public class AmandaMetaDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmandaMetaDataService.class);

    private Connection connection;

    private AttachmentMetaData attachmentMetaData;

    private FolderData folderData;

    private List<String> parcelData;

    private String permitData[][];

    private String[][] taxMetaData;

    public AmandaMetaDataService(Connection connection) {
        this.connection = connection;
    }

    public AttachmentMetaData getAttachmentMetaData(Integer attachmentRSN) throws SQLException {
        if (attachmentMetaData != null) {
            return attachmentMetaData;
        }
        PreparedStatement ps = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT IsNull(Attachment.attachmentdesc,'') Description, ");
            sql.append("ValidAttachment.AttachmentDesc Type ");
            sql.append("FROM Attachment inner join ValidAttachment ON Attachment.AttachmentCode = ValidAttachment.AttachmentCode ");
            sql.append("WHERE AttachmentRSN = ?");
            ps = connection.prepareStatement(sql.toString());
            ps.setLong(1, attachmentRSN);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                attachmentMetaData = new AttachmentMetaData();
                attachmentMetaData.setSource("AMANDA Attachment");
                attachmentMetaData.setDescription(rs.getString("Description"));
                attachmentMetaData.setType(rs.getString("Type"));
            }
            rs.close();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception exp) {
                    LOGGER.error("Error while closing prepared statement", exp);
                }
            }
        }
        return attachmentMetaData;
    }

    public FolderData getFolderData(Integer attachmentRSN) throws SQLException {
        if (folderData != null) {
            return folderData;
        }
        PreparedStatement ps = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("Select F.FolderType, F.FolderSection, F.FolderRevision, F.FolderSequence , F.FolderYear ");
            sql.append("From Folder F, Attachment A ");
            sql.append("where a.attachmentRSN = ? ");
            sql.append("and a.TableRSN=F.FolderRSN and a.TableName='Folder'");
            ps = connection.prepareStatement(sql.toString());
            ps.setLong(1, attachmentRSN);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                folderData = new FolderData();
                folderData.setFolderRevision(rs.getString("FolderRevision"));
                folderData.setFolderSequence(rs.getString("FolderSequence"));
                folderData.setFolderYear(rs.getString("FolderYear"));
                folderData.setFolderType(rs.getString("FolderType"));
                folderData.setFolderSection(rs.getString("FolderSection"));
            }
            rs.close();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception exp) {
                    LOGGER.error("Error while closing prepared statement", exp);
                }
            }
        }
        return folderData;
    }

    public List<String> getParcelInfo(Integer folderRSN) throws SQLException {
        if (parcelData != null) {
            return parcelData;
        }
        parcelData = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT distinct replace(p.PropertyRoll,'-','') ParcelID ");
            sql.append("FROM  Property as p , Folder as f, FolderProperty FP ");
            sql.append("WHERE P.PropertyRSN = FP.PropertyRSN ");
            sql.append("and F.FolderRSN=FP.FolderRSN and F.FolderRSN = ?");
            ps = connection.prepareStatement(sql.toString());
            ps.setInt(1, folderRSN);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String parcelId = rs.getString("ParcelID");
                if (parcelId != null && !parcelId.trim().isEmpty()) {
                    parcelData.add(parcelId);
                }
            }
            rs.close();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception exp) {
                    LOGGER.error("Error while closing prepared statement", exp);
                }
            }
        }
        return parcelData;
    }

    public String[][] getStandalonePermitData(FolderData folderData) throws SQLException {
        if (permitData != null) {
            return permitData;
        }
        permitData = new String[600][600];
        permitData[0][0] = "0";
        PreparedStatement ps = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("EXEC SNOCO_PRISM_Get_Permit_Complete_New ?, ?, ?, ?, ?");
            ps = connection.prepareStatement(sql.toString());
            ps.setString(1, folderData.getFolderType());
            ps.setString(2, folderData.getFolderSection());
            ps.setString(3, folderData.getFolderRevision());
            ps.setString(4, folderData.getFolderSequence());
            ps.setString(5, folderData.getFolderYear());
            ResultSet rs = ps.executeQuery();
            int i = 1;
            while (rs.next()) {
                for (int j = 0; j < 13; j++) {
                    permitData[i][j] = rs.getString(j + 1);
                }
                permitData[0][0]= Integer.toString(i);
                i++;
            }
            rs.close();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception exp) {
                    LOGGER.error("Error while closing prepared statement", exp);
                }
            }
        }
        return permitData;
    }

    public String[][] getTaxMetaDataInfo(String parcelNumber) throws SQLException {
        if (taxMetaData != null) {
            return taxMetaData;
        }
        taxMetaData = new String[600][20];
        PreparedStatement ps = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("EXEC SNOCO_PRISM_Get_Addresses ?");
            ps = connection.prepareStatement(sql.toString());
            ps.setString(1, parcelNumber);
            ResultSet rs = ps.executeQuery();
            int i = 1;
            while (rs.next()) {
                for (int j = 1; j < 8; j++) {
                    taxMetaData[i][j] = rs.getString(j);
                }
                if (taxMetaData[i][2].trim().contentEquals("")) {
                    taxMetaData[i][2] = "Address Unknown";
                }
                if (taxMetaData[i][3].trim().contentEquals("")) {
                    taxMetaData[i][3] = "City Unknown";
                }
                if (taxMetaData[i][4].trim().contentEquals("")) {
                    taxMetaData[i][4] = "Unk";
                }
                taxMetaData[0][0] = Integer.toString(i);
                i++;
            }
            rs.close();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception exp) {
                    LOGGER.error("Error while closing prepared statement", exp);
                }
            }
        }
        return taxMetaData;
    }
}
