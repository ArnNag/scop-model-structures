import java.sql.*;
import java.io.*;
import java.util.*;
import java.text.*;
import java.nio.file.*;
import java.util.stream.Stream;
import gov.lbl.scop.local.LocalSQL;
import org.rcsb.cif.*;
import org.rcsb.cif.model.*;
import org.rcsb.cif.schema.mm.*;
import org.rcsb.cif.schema.*;

public class MakeDBModelStructureUniprot {

    /** Represents a sequence read from a cif file and its Uniprot reference.
     */
    private static class CifSeq {

	String seq;
	String entryId; // Uniprot entry ID
	int unpStart; // zero-indexed, inclusive
	int unpEnd; // zero-indexed, inclusive - TODO: check this

        /** Read the relevant information from the cif file.
	 * @param p: The Path where the cif file exists.
         */
	private CifSeq(Path p) throws Exception {
	    CifFile cifFile = CifIO.readFromPath(p);
            MmCifFile mmCifFile = cifFile.as(StandardSchemata.MMCIF);
	    MmCifBlock data = mmCifFile.getFirstBlock();
	    this.seq = data.getEntityPoly().getPdbxSeqOneLetterCode().get(0).replaceAll("\n", "").toLowerCase();
	    StructRef structRef = data.getStructRef();
	    this.entryId = structRef.getDbCode().get(0); 
	    this.unpStart = Integer.parseInt(structRef.getPdbxAlignBegin().get(0)) - 1; // convert to zero-indexing
	    this.unpEnd = Integer.parseInt(structRef.getPdbxAlignEnd().get(0)) - 1; // convert to zero-indexing
            this.seq = seq;
	    this.entryId = entryId;
	    this.unpStart = unpStart;
	    this.unpEnd = unpEnd;
	}
    }

    /** Represents a sequence retrieved from the `uniprot` table and the associated primary key id.  */
    private static class DBSeq {
        String seq;
	String UNPid; // id in uniprot table

	private DBSeq(String seq, String UNPid) throws Exception {
	    this.seq = seq;
	    this.UNPid = UNPid;
	}

    }

    /** Retrieve all sequences associated with a Uniprot entry ID.
     * @param entryId: The Uniprot entry ID (field `long_id` in the uniprot table)
     */
    private static List<DBSeq> getDBseqs(String entryId) throws Exception {
        List<DBSeq> DBseqs = new ArrayList<>();	
        String query = "select uniprot.id, astral_seq.seq from uniprot join uniprot_seq on uniprot.id = uniprot_seq.uniprot_id join astral_seq on astral_seq.id = uniprot_seq.seq_id where uniprot.long_id = '" + entryId + "' order by uniprot.seq_date desc;";	
        ResultSet rs = doQuery(query);
        while (rs.next()) {
            DBseqs.add(new DBSeq(rs.getString("seq"), rs.getString("id")));
         }
	return DBseqs;
    }
    
    private static ResultSet doQuery(String query) throws Exception {
        Statement stmt = LocalSQL.createStatement();
	return stmt.executeQuery(query);
    }

    /** Adds an entry in `model_strucure_uniprot` for each  model structure in the
     * `model_structure` table with a sequence that matches that stored in 
     * the `uniprot` table. */
    public static void main(String[] args) {
	try {
            LocalSQL.connectRW();
            String modelPathQuery = "select id, cif_path from model_structure;";
	    ResultSet modelPathsRs = doQuery(modelPathQuery);
	    PreparedStatement insertModelUNP = LocalSQL.prepareStatement("insert into model_structure_uniprot (model_structure_id, uniprot_id, uniprot_start, uniprot_end) values (?, ?, ?, ?);"); 
	    while (modelPathsRs.next()) {
		int modelStructureId = modelPathsRs.getInt("id");
		Path cifPath = Paths.get(modelPathsRs.getString("cif_path"));
		CifSeq modelSeq = new CifSeq(cifPath);
	        List<DBSeq> DBseqs = getDBseqs(modelSeq.entryId);
		for (DBSeq dbSeq : DBseqs) { 
		    if (modelSeq.unpStart < dbSeq.seq.length()) {
		        String splicedDBseq = dbSeq.seq.substring(modelSeq.unpStart, modelSeq.unpEnd + 1);
			if (splicedDBseq.equals(modelSeq.seq)) {
			    insertModelUNP.setInt(1, modelStructureId);
			    insertModelUNP.setString(2, dbSeq.UNPid);
			    insertModelUNP.setInt(3, modelSeq.unpStart);
			    insertModelUNP.setInt(4, modelSeq.unpEnd);
			    insertModelUNP.executeUpdate();
			    break;
			}
		    }
		}
	    }
	} catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

}
