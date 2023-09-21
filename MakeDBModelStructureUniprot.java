import java.sql.*;
import java.io.*;
import java.util.*;
import java.text.*;
import java.nio.file.*;
import java.util.stream.Stream;
import org.strbio.IO;
import org.strbio.io.*;
import org.strbio.math.*;
import org.strbio.mol.*;
import org.strbio.util.*;
import gov.lbl.scop.local.LocalSQL;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.rcsb.cif.*;
import org.rcsb.cif.model.*;
import org.rcsb.cif.schema.mm.*;
import org.rcsb.cif.schema.*;

public class MakeDBModelStructureUniprot {

    public static void main(String[] args) {
	try {
            LocalSQL.connectRW();
            String model_path_query = "select id, cif_path from model_structure;";
	    ResultSet model_paths_rs = doQuery(model_path_query);
	    PreparedStatement insertModelUNP = LocalSQL.prepareStatement("insert into model_structure_uniprot (model_structure_id, uniprot_id, uniprot_start, uniprot_end) values (?, ?, ?, ?);"); 
	    while (model_paths_rs.next()) {
		int model_structure_id = model_paths_rs.getInt("id");
		String cif_path = model_paths_rs.getString("cif_path");
		Path cif_path_p = Paths.get(cif_path);
		CifInfo modelInfo = new CifInfo(cif_path_p);
	        List<List<String>> DBseqs = getDBseqs(modelInfo.entryId);
		boolean match = false;
		int numDBseq = 0;
		while (numDBseq < DBseqs.get(0).size()) {
		    String fullDBseq = DBseqs.get(0).get(numDBseq);
		    String UNPid = DBseqs.get(1).get(numDBseq);

		    if (modelInfo.unpStart < fullDBseq.length()) {
		        String splicedDBseq = fullDBseq.substring(modelInfo.unpStart, modelInfo.unpEnd + 1);
	       	        match = splicedDBseq.equals(modelInfo.seq);
			if (match) {
			    insertModelUNP.setInt(1, model_structure_id);
			    insertModelUNP.setString(2, UNPid);
			    insertModelUNP.setInt(3, modelInfo.unpStart);
			    insertModelUNP.setInt(4, modelInfo.unpEnd);
			    insertModelUNP.executeUpdate();
			    break;
			}
		    }
		    numDBseq++;
		}
	    }
	} catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

    private static class CifInfo {
        String seq;
	String entryId;
	int unpStart;
	int unpEnd;

	private CifInfo(Path p) throws Exception {
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

    private static List<List<String>> getDBseqs(String entryId) throws Exception {
        List<String> seqs = new ArrayList<String>();	
        List<String> UNPids = new ArrayList<String>();
        String query = "select uniprot.id, astral_seq.seq from uniprot join uniprot_seq on uniprot.id = uniprot_seq.uniprot_id join astral_seq on astral_seq.id = uniprot_seq.seq_id where uniprot.long_id = '" + entryId + "' order by uniprot.seq_date desc;";	
	ResultSet rs = doQuery(query);
	while (rs.next()) {
	    seqs.add(rs.getString("seq"));
	    UNPids.add(rs.getString("id"));
	}
	return List.of(seqs, UNPids);
    }

    
    private static ResultSet doQuery(String query) throws Exception {
        Statement stmt = LocalSQL.createStatement();
	return stmt.executeQuery(query);
    }

    private static List<String> rsToList(ResultSet rs, String column) throws SQLException {
        List<String> resultList = new ArrayList<String>();
        while (rs.next()) {
	    resultList.add(rs.getString(column));
	}
	return resultList;
    }

}
