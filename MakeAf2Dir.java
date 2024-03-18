import java.io.IOException;
import java.nio.file.*;
import java.util.List;

public class MakeAf2Dir {

	// Set these to the appropriate paths.
	static Path af2Download = Paths.get("/h/anagle/populate_model_structure/af2_new");
	static Path af2Out = Paths.get("/h/anagle/zippedAf2");
	static Path cppDictPackBuild = Paths.get("/h/anagle/cif2xml/cpp-dict-pack/build");
	static String dictName = "mmcif_ma"; // the name of the dictionary that the CIF files were constructed with


    public static void main(String[] args) {
	Path af2Pdb = af2Out.resolve("pdb");
	Path af2Cif = af2Out.resolve("cif");
	Path af2Xml = af2Out.resolve("xml");
	Path dictPath = cppDictPackBuild.resolve("odb").resolve(String.format("%s.odb", dictName));
	Path convertScript = cppDictPackBuild.resolve("bin").resolve("mmcif2XML");

        try {
            Files.createDirectories(af2Out);
            Files.createDirectories(af2Pdb);
            Files.createDirectories(af2Cif);
            Files.createDirectories(af2Xml);

            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(af2Download);
            for (Path file : directoryStream) {
	        String[] fileNameParts = file.getFileName().toString().split("-");
                if (fileNameParts.length >= 2) {
			String UnpAccession = fileNameParts[1];
			String hash = UnpAccession.substring(UnpAccession.length() - 3, UnpAccession.length() - 1);
			if (file.getFileName().toString().endsWith(".pdb.gz")) {
				Path af2PdbHash = af2Pdb.resolve(hash);
				Files.createDirectories(af2PdbHash);
				Files.copy(file, af2PdbHash.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                } else if (file.getFileName().toString().endsWith(".cif.gz")) {
                        Path af2CifHash = af2Cif.resolve(hash);
                        Path af2XmlHash = af2Xml.resolve(hash);
                        Files.createDirectories(af2CifHash);
                        Files.createDirectories(af2XmlHash);

			Files.copy(file, af2CifHash.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);

			// Unzip the cif file
			ProcessBuilder gunzipProcess = new ProcessBuilder("gunzip", file.toString());
			System.out.println(gunzipProcess.command());
			Path unzippedCif = af2Download.resolve(file.getFileName().toString().replace(".gz", ""));
                        // Run the conversion script
                        ProcessBuilder convertProcess = new ProcessBuilder(convertScript.toString(), "-df", dictPath.toString(), "-dictName", String.format("%s.dic", dictName), "-prefix", "pdbx-v50", "-ns", "PDBx", "-f", unzippedCif.toString(), "-v");
			System.out.println(convertProcess.command());
                        convertProcess.directory(af2Download.toFile());
                        Process convert = convertProcess.start();
                        convert.waitFor();

                        // Zip the resulting file to a file that ends in .xml.gz
                        String convertedXML = af2Download.resolve(file.getFileName()).toString().replace(".cif.gz", ".cif.xml");
                        String xmlTarget = af2XmlHash.resolve(file.getFileName()).toString().replace(".cif.gz", ".xml.gz");
			ProcessBuilder gzipProcess = new ProcessBuilder("zcat", convertedXML, ">", xmlTarget);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

