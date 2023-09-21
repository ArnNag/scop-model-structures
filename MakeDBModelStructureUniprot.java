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
		String[] sequenceEntryId = getCifInfo(cif_path_p);
		String cifSeq = sequenceEntryId[0];
		String unpEntryId = sequenceEntryId[1];
		int unpStartZero = Integer.parseInt(sequenceEntryId[2]) - 1;
		int unpEndZero = Integer.parseInt(sequenceEntryId[3]) - 1;
	        // System.out.println("Entry ID: " + unpEntryId);
		// System.out.println("cif seq: " + cifSeq);
	        List<List<String>> DBseqs = getDBseqs(unpEntryId);
		boolean match = false;
		int numDBseq = 0;
		while (numDBseq < DBseqs.get(0).size()) {
		    String fullDBseq = DBseqs.get(0).get(numDBseq);
		    String UNPid = DBseqs.get(1).get(numDBseq);
	            // System.out.println("astral seq full: " + fullDBseq);
		    // System.out.println("unpStartZero: " + unpStartZero);
		    // System.out.println("unpEndZero: " + unpEndZero);

		    if (unpEndZero < fullDBseq.length()) {
		        String splicedDBseq = fullDBseq.substring(unpStartZero, unpEndZero + 1);
	                // System.out.println("astral seq spliced: " + splicedDBseq);
	       	        match = splicedDBseq.equals(cifSeq);
			if (match) {
			    // System.out.println("match: " + match);
			    insertModelUNP.setInt(1, model_structure_id);
			    insertModelUNP.setString(2, UNPid);
			    insertModelUNP.setInt(3, unpStartZero);
			    insertModelUNP.setInt(4, unpEndZero);
			    // System.out.println(insertModelUNP);
			    insertModelUNP.executeUpdate();
			    break;
			}
		    }
		    numDBseq++;
		}
		// System.out.println("");
	    }
	} catch (Exception e) {
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

    private static String[] getCifInfo(Path p) throws Exception {
	CifFile cifFile = CifIO.readFromPath(p);
	MmCifFile mmCifFile = cifFile.as(StandardSchemata.MMCIF);
	MmCifBlock data = mmCifFile.getFirstBlock();
        String seq = data.getEntityPoly().getPdbxSeqOneLetterCode().get(0).replaceAll("\n", "").toLowerCase();
	StructRef structRef = data.getStructRef();
	String entryId = structRef.getDbCode().get(0); 
	String unpStart = structRef.getPdbxAlignBegin().get(0);
	String unpEnd = structRef.getPdbxAlignEnd().get(0);
	return new String[] {seq, entryId, unpStart, unpEnd};
    }

    
    
    // Look for the Uniprot accession number in uniprot_accession. Return a list of all uniprot_ids that match. 
    private static List<String> getLatestUNPids(String accession) throws Exception {
        String query = "select uniprot_id from uniprot_accession, uniprot where uniprot.id = uniprot_id and uniprot.is_obsolete = 0 and accession = '" + accession + "';";
	ResultSet unpIDrs = doQuery(query);
	return rsToList(unpIDrs, "uniprot_id");
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
