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

public class MakeDBModelStructure {

    public static void main(String[] args) {
	try {
            LocalSQL.connectRW();
            PreparedStatement stmt1 = LocalSQL.prepareStatement("insert into model_structure (source_id, pdb_path, cif_path, header_date, file_date) values (1, ?, ?, 20220601, 20220505);");
	    String af2_directory = "/mnt/net/ipa.jmcnet/data/h/anagle/af2_human_v3";
	    Stream<Path> files = Files.walk(Paths.get(af2_directory)).filter(x -> x.getFileName().toString().contains("pdb"));
	    for (Path p : (Iterable<Path>)files::iterator) {
                String pdb_path = p.toString();
		String cif_path = pdb_path.substring(0, pdb_path.length() - 7) + ".cif.gz";
		stmt1.setString(1, pdb_path);
		stmt1.setString(2, cif_path);
		stmt1.executeUpdate();
	    }
	}
        catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
            e.printStackTrace();
        }
    }

}
