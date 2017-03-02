package db;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.sis.measure.Range;
import org.apache.sis.util.collection.RangeSet;

import model.Parameters;
import parsing.model.Gene;
import tools.Utils;

public class DBManager 
{
	public static String JDBC_DRIVER = null;
	public static String URL = null;
	public static Connection conn = null;
	public static FTPClient f = null;
	
	public static HashMap<String, HashMap<Integer, String>> urls = new HashMap<String, HashMap<Integer, String>>();
	
	public static HashMap<String, GeneInfo> geneInfo = new HashMap<>();
	
	public static final String[] species = new String[] {"ailuropoda_melanoleuca","anas_platyrhynchos","anolis_carolinensis","astyanax_mexicanus","bos_taurus","caenorhabditis_elegans","callithrix_jacchus","canis_familiaris","cavia_porcellus","chlorocebus_sabaeus","choloepus_hoffmanni","ciona_intestinalis","ciona_savignyi","danio_rerio","dasypus_novemcinctus","dipodomys_ordii","drosophila_melanogaster","echinops_telfairi","equus_caballus","erinaceus_europaeus","felis_catus","ficedula_albicollis","gadus_morhua","gallus_gallus","gasterosteus_aculeatus","gorilla_gorilla","homo_sapiens","ictidomys_tridecemlineatus","latimeria_chalumnae","lepisosteus_oculatus","loxodonta_africana","macaca_mulatta","macropus_eugenii","meleagris_gallopavo","microcebus_murinus","monodelphis_domestica","mus_musculus","mustela_putorius_furo","myotis_lucifugus","nomascus_leucogenys","ochotona_princeps","oreochromis_niloticus","ornithorhynchus_anatinus","oryctolagus_cuniculus","oryzias_latipes","otolemur_garnettii","ovis_aries","pan_troglodytes","papio_anubis","pelodiscus_sinensis","petromyzon_marinus","poecilia_formosa","pongo_abelii","procavia_capensis","pteropus_vampyrus","rattus_norvegicus","saccharomyces_cerevisiae","sarcophilus_harrisii","sorex_araneus","sus_scrofa","taeniopygia_guttata","takifugu_rubripes","tarsius_syrichta","tetraodon_nigroviridis","tupaia_belangeri","tursiops_truncatus","vicugna_pacos","xenopus_tropicalis","xiphophorus_maculatus"};
	
	public static void main(String[] args) throws Exception
	{
		readSpecies(); // Read our precomputed file
		String spe = "mus_musculus";
		
		System.out.println("Computing gene info for species " + spe + " ...");
		HashMap<Integer, String> toLoad = urls.get(spe);
		for(int i = 43; i <= 87; i++) // order is important
		{
			String url = toLoad.get(i);
			if(!url.equals("NA"))
			{
				downloadEnsembl(toLoad.get(i));
				System.out.println("Release " + i + ": "+geneInfo.size()+" genes now in database.");
			}
			else System.out.println("Release " + i + " does not exist for species " + spe);
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(spe + ".txt"));
		bw.write("Ensembl\tName\tAltNames\tBiotype\tGeneLength\tSumExonLength\n");
		ArrayList<String> tmp = new ArrayList<>();
		for(String gene_id:geneInfo.keySet()) tmp.add(gene_id);
		Collections.sort(tmp);
		for(String gene_id:tmp)
		{
			GeneInfo g = geneInfo.get(gene_id);
			bw.write(gene_id + "\t" + g.gene_name + "\t" + buildAltNamesString(g.alternate_names, g.gene_id.toUpperCase(), g.gene_name.toUpperCase()) + "\t" + g.type + "\t" + (g.end - g.start + 1) + "\t" + g.getLength() + "\n");
		}
		bw.close();
	}
	
	private static String buildAltNamesString(HashSet<String> names, String ensNameUp, String geneNameUp)
	{
		HashSet<String> unique = new HashSet<String>();
		String res = "";
		for(String n:names)
		{
			String nUp = n.toUpperCase();
			if(!unique.contains(nUp))
			{
				if(!nUp.equals(ensNameUp) && !nUp.equals(geneNameUp)) res += n + ","; // Do not take if doublon, or same ens, or same gene name
				unique.add(nUp);
			}
		}
		if(res.endsWith(",")) res = res.substring(0, res.length() - 1); // Remove last comma
		return res;
	}
	
	public static void readSpecies() throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader("species.txt"));
		String line = br.readLine();
		HashMap<Integer, Integer> lineToRelease = new HashMap<>();
		String[] header = line.split("\t");
		for(int i = 1; i < header.length; i++) lineToRelease.put(i, Integer.parseInt(header[i]));
		line = br.readLine();
		while(line != null) // One line per specie
		{
			String[] tokens = line.split("\t");
			HashMap<Integer, String> urlss = new HashMap<Integer, String>();
			for(int i = 1; i < tokens.length; i++) urlss.put(lineToRelease.get(i), tokens[i]);
			urls.put(tokens[0], urlss);
			line = br.readLine();
		}
		br.close();
	}
	
	public static void downloadEnsembl(String path)
	{
		try 
		{
			int i1 = path.indexOf("release-");
			int release = Integer.parseInt(path.substring(i1+8, path.indexOf("/", i1)));
			URL url = new URL("ftp://ftp.ensembl.org"+path);
			InputStream is = url.openStream();
	        InputStream gzipStream = new GZIPInputStream(is);
	        BufferedReader br = new BufferedReader(new InputStreamReader(gzipStream)); 
		    
	        for(String key:geneInfo.keySet()) geneInfo.get(key).exon_id = RangeSet.create(Long.class, true, true); // This needs to be reset for every release
	        
	    	String line = br.readLine();
	    	while(line != null)
	    	{
	    		if(!line.startsWith("#"))
    			{
    				String[] tokens = line.split("\t");
    				// Parse Line
 					String[] params = tokens[8].split(";");
					long start = Long.parseLong(tokens[3]);
					long end = Long.parseLong(tokens[4]);
					String gene_name = null;
					String gene_id = null;
					String biotype = null;
					String type = tokens[2];
					for(String param:params) 
					{
						String value = param.substring(param.indexOf("\"")+1, param.lastIndexOf("\""));
						if(param.contains("gene_name")) gene_name = value;
						if(param.contains("gene_id")) gene_id = value;
						if(param.contains("gene_biotype")) biotype = value;
					}
					if(gene_name == null) gene_name = gene_id;
					if(biotype == null) biotype = tokens[1]; // earlier versions have biotype in the second column
					if(gene_id.startsWith("ENS")) // Weird things are not computed
					{
						// Is it already existing in the database?
						GeneInfo g = geneInfo.get(gene_id);
						if(g == null) 
						{
							g = new GeneInfo();
							g.firstRelease = release;
							g.gene_id = gene_id; // Will never change
							g.gene_name = gene_name;
						}
						else // If this EnsemblID already in the database
						{
							if(!gene_name.equals(g.gene_name)) // If new name
							{
								g.alternate_names.add(g.gene_name); // Add the old name
								g.gene_name = gene_name;
							}
						}
						g.type = biotype; // Update (do not save previous)
						// Which type is it?
						if(type.equals("gene"))
						{
							// I update the length of the gene only according to the 'gene' tag (not working for earlier versions, since the gene tag was not existing). But if I go through everything, it should work.
							g.end = end; 
							g.start = start;
						}
						else if(type.equals("exon")) g.exon_id.add(start, end); // This needs to be flushed for each release
	    				geneInfo.put(g.gene_id, g);
					}
    			}
    			line = br.readLine();
    		}
    		br.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	private static void generateSpeciesURLFromEnsembl() throws Exception
	{
		f = new FTPClient();
		f.connect("ftp.ensembl.org");
		f.login("anonymous","");
		for(int i = 43; i <= 87; i++) getGTF(i);
		ArrayList<String> species = new ArrayList<String>();
		for(String s:urls.keySet()) species.add(s);
		Collections.sort(species);
		BufferedWriter bw = new BufferedWriter(new FileWriter("species.txt"));
		bw.write("species");
		for(int i = 43; i <= 87; i++) bw.write("\t" + i);
		bw.write("\n");
		for(String s:species)
		{
			bw.write(s);
			HashMap<Integer, String> releases = urls.get(s);
			for(int i = 43; i <= 87; i++) 
			{
				String url = releases.get(new Integer(i));
				if(url == null) url = "NA";
				bw.write("\t"+url);
			}
			bw.write("\n");
		}
		bw.close();
	}
	
	private static void getGTF(int release) throws Exception // Some disappears from Ensembl => VectorBase (for e.g. some insects)
	{
		if(release >= 48) getGTF_48_87(release);
		else if(release == 47) getGTF_47(release);
		else if(release >= 43) getGTF_43_46(release);
		else System.exit(-1);
	}
	
	private static void getGTF_48_87(int release) throws Exception // 48-87 // Jusqu'a r80 inclus, un seul fichier gtf
	{

		FTPFile[] files = f.listDirectories("/pub/release-"+release+"/gtf/");
		for(FTPFile fi:files)
		{
			String url = "/pub/release-"+release+"/gtf/"+fi.getName()+"/";
			FTPFile[] subfiles = f.listFiles(url);
			for(FTPFile fo:subfiles)
			{
				if(fo.getName().endsWith("gtf.gz") && fo.getName().indexOf("abinitio") == -1 && fo.getName().indexOf(".chr") == -1)
				{
					HashMap<Integer, String> releases = urls.get(fi.getName());
					if(releases == null) releases = new HashMap<Integer, String>();
					releases.put(release, url+fo.getName());
					urls.put(fi.getName(), releases);
				}
			}
		}
	}
	
	private static void getGTF_47(int release) throws Exception // 47
	{
		String url = "/pub/release-"+release+"/gtf/";
		FTPFile[] files = f.listFiles(url);
		for(FTPFile fi:files)
		{
			String name = fi.getName();
			name= name.substring(0, name.indexOf(".")).toLowerCase();
			HashMap<Integer, String> releases = urls.get(name);
			if(releases == null) releases = new HashMap<Integer, String>();
			releases.put(release, url+fi.getName());
			urls.put(name, releases);
		}
	}
	
	
	private static void getGTF_43_46(int release) throws Exception // 43-46
	{
		FTPFile[] files = f.listDirectories("/pub/release-"+release);
		for(FTPFile fi:files)
		{
			String url = "/pub/release-"+release+"/"+fi.getName()+"/data/gtf/";
			FTPFile[] subfiles = f.listFiles(url);
			for(FTPFile fo:subfiles)
			{
				String name = fi.getName();
				name = name.substring(0, name.indexOf("_", name.indexOf("_") + 1)).toLowerCase();
				HashMap<Integer, String> releases = urls.get(name);
				if(releases == null) releases = new HashMap<Integer, String>();
				releases.put(release, url+fo.getName());
				urls.put(name, releases);
			}
		}
	}
	
	public static void generateEnrichmentDB()
	{
		try 
		{
			DrugBankParser.parse("hsa", "C:/Users/gardeux/Dropbox/GeneSets/DrugBank/drugbank.xml", "C:/Users/gardeux/Dropbox/ASAP/Scripts/Enrichment/Genesets/drugbank_hsa.gmt");
			DrugBankParser.parse("mmu", "C:/Users/gardeux/Dropbox/GeneSets/DrugBank/drugbank.xml", "C:/Users/gardeux/Dropbox/ASAP/Scripts/Enrichment/Genesets/drugbank_mmu.gmt");
			KEGGRestApi.generateKEGGDB("hsa", "C://Users/Vincent/Dropbox/ASAP/Scripts/Enrichment/Genesets/kegg_hsa.gmt"); // mmu, rno (rat), dme (droso mela)
			KEGGRestApi.generateKEGGDB("mmu", "C://Users/Vincent/Dropbox/ASAP/Scripts/Enrichment/Genesets/kegg_mmu.gmt"); // mmu, rno (rat), dme (droso mela)
			GeneAtlas.enrichrToGMT("C:/Users/Vincent/Desktop/GeneSets/GeneAtlas/Human_Gene_Atlas.txt", "C://Users/Vincent/Dropbox/ASAP/Scripts/Enrichment/Genesets/gene_atlas.hsa.gmt");
			GeneAtlas.enrichrToGMT("C:/Users/Vincent/Desktop/GeneSets/GeneAtlas/Mouse_Gene_Atlas.txt", "C://Users/Vincent/Dropbox/ASAP/Scripts/Enrichment/Genesets/gene_atlas.mmu.gmt");
			GODatabase.generateGODB("C:/Users/gardeux/Dropbox/ASAP/Scripts/Enrichment/Genesets/", "hsa");
			GODatabase.generateGODB("C:/Users/gardeux/Dropbox/ASAP/Scripts/Enrichment/Genesets/", "mmu");
		} 
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public static void createDB()
	{
		long t = System.currentTimeMillis();
		connect();
		System.out.print("Running: DROP TABLE genes...");
		dropTable("genes");
		System.out.println("DONE!");
		
		System.out.print("Running: CREATE TABLE genes...");
		createGeneTable();
		System.out.println("DONE!");
		disconnect();
		System.out.println("Creating DB took " + Utils.toReadableTime(System.currentTimeMillis() - t));
	}
	
	/*public static void populateDB()
	{
		// Release 43 is first release to have GTF files
		long t = System.currentTimeMillis();
		connect();
		HashMap<String, GeneInfo> geneInfo = downloadEnsembl(87);
		System.out.println("Found "+geneInfo.size()+" genes.");
		for(String gene_id:geneInfo.keySet())
		{
			GeneInfo g = geneInfo.get(gene_id);
			insertGene(gene_id, g.gene_name, 1, g.type, (int)g.getLength(), (int)(g.end - g.start + 1));
		}
		
		disconnect();
		System.out.println("Populating DB took " + Utils.toReadableTime(System.currentTimeMillis() - t));
	}*/
	
	public static void createGeneTable()
	{
		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();
			String sql = "CREATE TABLE genes " +
					"(id INTEGER NOT NULL AUTO_INCREMENT, " +
					" ensembl_id TEXT, " + 
					" name TEXT, " + 
					" alternate_names TEXT, " +
					" organism_id INTEGER DEFAULT NULL, " +
					" biotype TEXT, " +
					" exon_length INTEGER, " +
					" full_length INTEGER, " +
					" created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
					" PRIMARY KEY ( id ))";
			stmt.executeUpdate(sql);
		}
		catch(Exception e) { e.printStackTrace(); }
		finally
		{
			try
			{
				if(stmt!=null) stmt.close();
			}
			catch(SQLException se2){ }// nothing we can do
		}
	}
	
	public static void dropTable(String name)
	{
		Statement stmt = null;
		try
		{
			  stmt = conn.createStatement();
			  stmt.executeUpdate("DROP TABLE " + name);
		}
		catch(Exception e) { e.printStackTrace(); }
		finally
		{
			try
			{
				if(stmt!=null) stmt.close();
			}
			catch(SQLException se2){ }// nothing we can do
		}
	}
	
	public static void insertGene(String ensembl_id, String name, String alternateNames, int organism_id, String biotype, int exon_length, int full_length)
	{
		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();
			String sql = "INSERT INTO genes (ensembl_id, name, alternate_names, organism_id, biotype, exon_length, full_length) VALUES ('"+ensembl_id+"','"+name+"','"+alternateNames+"',"+organism_id+",'"+biotype+"',"+exon_length+","+full_length+")";
			stmt.executeUpdate(sql);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(stmt!=null) stmt.close();
			}
			catch(SQLException se2){ }// nothing we can do
		}
	}

	public static HashMap<String, ArrayList<Gene>> getGenesInDB()
	{
		HashMap<String, ArrayList<Gene>> genes = new HashMap<>(); // Hopefully it is quick enough to build?? If not it should not be stored.
		connect();
		
		// I perform my request
		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();
		
			String sql = "SELECT * FROM genes WHERE organism_id="+Parameters.organism; // Restrict to genes of this organism
			ResultSet rs = stmt.executeQuery(sql);
			 
				// I check the results and compare to my gene list
			while(rs.next())
			{
				Gene g = new Gene();
				g.id = rs.getInt("id");
				g.ensembl_id = rs.getString("ensembl_id");
				g.name= rs.getString("name");
// TODO				g.alternate_names = rs.getString("alternate_names");
				g.organism_id = rs.getInt("organism_id");
				g.created_at = rs.getString("created_at");
				String ensUp = g.ensembl_id.toUpperCase();
				String nameUp = g.name.toUpperCase();
				// I add this gene twice in the HashMap. Once its ensembl_id ...
				ArrayList<Gene> gene_list = genes.get(ensUp);
				if(gene_list == null) gene_list = new ArrayList<>();
				gene_list.add(g);
				genes.put(ensUp, gene_list);
				// ... and once the gene name
				gene_list = genes.get(nameUp);
				if(gene_list == null) gene_list = new ArrayList<>();
				gene_list.add(g);
				genes.put(nameUp, gene_list);
				// TODO: add alternate gene names??
			}
			rs.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(stmt!=null) stmt.close();
			}
			catch(SQLException se2){ }// nothing we can do
		}
		disconnect();
		return genes;
	}

	public static void connect()
	{
		try
		{
			Class.forName(JDBC_DRIVER);
			System.out.print("Connecting to database...");
			conn = DriverManager.getConnection(URL);
			System.out.println("Connected!");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void disconnect()
	{
		try
		{
			if(conn!=null) conn.close();
		}
		catch(SQLException se)
		{
			se.printStackTrace();
		}
	}
}

class GeneInfo
{
	public int firstRelease;
	public String gene_id; // Main ID
	public String gene_name;
	public HashSet<String> alternate_names = new HashSet<String>();
	public String type = null;
	public long start;
	public long end;
	public RangeSet<Long> exon_id = RangeSet.create(Long.class, true, true);
	
	public long getLength()
	{
		long sum = 0;
		for(Range<Long> r:exon_id) sum += ((Long)r.getMaxValue() - (Long)r.getMinValue() + 1);
		return sum;
	}
}
