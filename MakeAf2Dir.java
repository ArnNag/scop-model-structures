import java.io.IOException;
import java.nio.file.*;
import java.util.List;

public class MakeAf2Dir {

	// Set these to the appropriate paths.
	static Path af2Download = Paths.get("/h/anagle/populate_model_structure/UP000005640_9606_HUMAN_v4");
	static Path af2Out = Paths.get("/h/anagle/zippedAf2");
	static Path cppDictPackBuild = Paths.get("/h/anagle/cif2xml/cpp-dict-pack/build");
	static String dictName = "mmcif_ma"; // the name of the dictionary that the CIF files were constructed with


    private static String unzipAndCopy(Path zipped, Path targetDir) throws IOException, InterruptedException {
                        ProcessBuilder gunzipProcess = new ProcessBuilder("gunzip", zipped.toString());
                        gunzipProcess.directory(af2Download.toFile());
                        Process gunzip = gunzipProcess.start();
                        gunzip.waitFor();

                        String unzipped = zipped.getFileName().toString().replace(".gz", "");
			Files.copy(af2Download.resolve(unzipped), targetDir.resolve(unzipped), StandardCopyOption.REPLACE_EXISTING);
			return unzipped;

    }

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
				unzipAndCopy(file, af2PdbHash);
                } else if (file.getFileName().toString().endsWith(".cif.gz")) {
                        Path af2CifHash = af2Cif.resolve(hash);
                        Path af2XmlHash = af2Xml.resolve(hash);
                        Files.createDirectories(af2CifHash);
                        Files.createDirectories(af2XmlHash);


                        String unzippedCif = unzipAndCopy(file, af2CifHash);
                        // Run the conversion script
                        ProcessBuilder convertProcess = new ProcessBuilder(convertScript.toString(), "-df", dictPath.toString(), "-dictName", String.format("%s.dic", dictName), "-prefix", "pdbx-v50", "-ns", "PDBx", "-f", unzippedCif, "-v");
			System.out.println(convertProcess.command());
                        convertProcess.directory(af2Download.toFile());
                        Process convert = convertProcess.start();
                        convert.waitFor();

                        // Rename the resulting file to .xml
                        Path convertedFileName = af2Download.resolve(unzippedCif.replace(".cif", ".cif.xml"));
                        Path xmlFileName = af2XmlHash.resolve(unzippedCif.replace(".cif", ".xml"));
                        Files.move(convertedFileName, xmlFileName, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

