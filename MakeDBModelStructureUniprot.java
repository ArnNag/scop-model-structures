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
		String accession = getAccession(cif_path);
		boolean hasNonObsolete = false;
		boolean match = false;
		boolean hasSeq = false;
		List<String> DBseq;
		List<String> UNPids = getUNPids(accession); 
		String UNPid = "-1";
		boolean hasUNPids = (UNPids.size() != 0);
		String DBseqstr = "";
		System.out.println("hasUNPids: " + hasUNPids);
		String cifSeq = getSequence(cif_path_p).toLowerCase();
		System.out.println("cif seq: " + cifSeq);
		if (hasUNPids) {
		    String fileSeq = getSequence(cif_path_p);
		    List<String> nonObsolete = getLatestUNPids(accession);
		    if (nonObsolete.size() != 0) {
			hasNonObsolete = true;
			UNPid = nonObsolete.get(0);
		    } else {
			UNPid = UNPids.get(0);
		    }
		    DBseq = getDBseq(UNPid);
		    System.out.println("cif seq: " + cifSeq);
		    hasSeq = (DBseq.size() != 0);
		    if (hasSeq) {
			DBseqstr = DBseq.get(0); 
			// if (isFrag) {
			//     DBseqstr = getFragSubseq(fragNum, DBseqstr);     
			// } 
			match = (DBseqstr.equals(cifSeq));
		    }
		}

		System.out.println("match: " + match);
		
		if (match) {
			insertModelUNP.setInt(1, model_structure_id);
			insertModelUNP.setString(2, UNPid);
			insertModelUNP.setInt(3, 1);
			insertModelUNP.setInt(4, 1);
			// System.out.println(insertModelUNP);
			// insertModelUNP.executeUpdate();
		}
	    }
		// stmt2.setString(1, pdb_path);
		// stmt2.setString(2, cif_path);
		// stmt1.executeUpdate();
		// String accession = getAccession(p);
		// boolean isFrag = isFrag(p);
		// int fragNum = getFragNum(p);
		// if (isFrag) {
		// 	boolean hasNonObsolete = false;
		// 	boolean match = false;
		// 	boolean hasSeq = false;
		// 	List<String> DBseq;
		// 	List<String> UNPids = getUNPids(accession); 
		// 	boolean hasUNPids = (UNPids.size() != 0);
		// 	String DBseqstr = "";
		// 	String PDBseq = getSequence(p).toLowerCase();
		// 	if (hasUNPids) {
		// 	    String fileSeq = getSequence(p);
		// 	    List<String> nonObsolete = getLatestUNPids(accession);
		// 	    if (nonObsolete.size() != 0) {
		// 		hasNonObsolete = true;
		// 		DBseq = getDBseq(nonObsolete.get(0));
		// 	    } else {
		// 		DBseq = getDBseq(UNPids.get(0));
		// 	    }
		// 	    hasSeq = (DBseq.size() != 0);
		// 	    if (hasSeq) {
		// 		DBseqstr = DBseq.get(0); 
		// 		if (isFrag) {
		// 		    DBseqstr = getFragSubseq(fragNum, DBseqstr);     
		// 		} 
		// 		match = (DBseqstr.equals(PDBseq));
		// 	    }
		// 	}
		    // }
	}
        catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

    // Extract the Uniprot accession number from the file name.
    private static String getAccession(String fileName) {
        return fileName.replaceAll(".*AF-", "").replaceAll("-.*", "");
    }

    private static String getAccession(Path p) {
        return getAccession(p.getFileName().toString());
    }

    private static String getSequence(Path p) throws Exception {
	CifFile cifFile = CifIO.readFromPath(p);
	MmCifFile mmCifFile = cifFile.as(StandardSchemata.MMCIF);
	MmCifBlock data = mmCifFile.getFirstBlock();
        return data.getEntityPoly().getPdbxSeqOneLetterCode().get(0);
    }
    
    // Look for the Uniprot accession number in uniprot_accession. Return a list of all uniprot_ids that match. 
    private static List<String> getLatestUNPids(String accession) throws Exception {
        String query = "select uniprot_id from uniprot_accession, uniprot where uniprot.id = uniprot_id and uniprot.is_obsolete = 0 and accession = '" + accession + "';";
	ResultSet unpIDrs = doQuery(query);
	return rsToList(unpIDrs, "uniprot_id");
    }


    private static List<String> getDBseq(String UNPid) throws Exception {
        String query = "select seq, astral_seq.id from uniprot_seq, astral_seq where uniprot_id = " + UNPid + " and seq_id = astral_seq.id;";
	return rsToList(doQuery(query), "seq");
    }

    private static boolean isFrag(Path p) {
        String fileName = p.getFileName().toString();
	String F2fileName = fileName.replaceFirst("-F\\d*-", "-F2-");
	Path F2path = p.resolveSibling(Paths.get(F2fileName));
	return Files.exists(F2path);
    }

    private static int getFragNum(Path p) {
        String fileName = p.getFileName().toString();
	Pattern fragPattern = Pattern.compile("-F\\d*-");
	Matcher m = fragPattern.matcher(fileName);
	if (m.find()) {
		String fragMatch =  m.group();
		return Integer.parseInt(fragMatch.substring(2, fragMatch.length() - 1));
	}
	return 0;

    }

    private static String getFragSubseq(int fragNum, String seq) {
	    int fragIncrement = 200;
	    int fragSize = 1400;
	    int fragStart = (fragNum - 1) * fragIncrement;
	    if (fragStart >= seq.length()) {
		    return "out of range";
	    }
	    int fragEnd = Integer.min(fragStart + fragSize, seq.length());
	    return seq.substring(fragStart, fragEnd);
    }
    
    // Look for the Uniprot accession number in uniprot_accession. Return a list of all uniprot_ids that match. 
    private static List<String> getUNPids(String accession) throws Exception {
        String query = "select uniprot_id from uniprot_accession where accession = '" + accession + "';";
	ResultSet unpIDrs = doQuery(query);
	return rsToList(unpIDrs, "uniprot_id");
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
