package db;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.ProgressMonitor;

import tools.Utils;

public class GODatabase 
{
   private static HashMap<String, GOTerm> termsBP = null;
   private static HashMap<String, GOTerm> termsMF = null;
   private static HashMap<String, GOTerm> termsCC = null;
   

   public static void generateGODBv2(String outputGMTFolder, String species) throws IOException
   {
	   int taxonId = 0;
	   if(species.equals("hsa")) taxonId = 9606;
	   if(species.equals("mmu")) taxonId = 10090;
	   if(!outputGMTFolder.endsWith("/")) outputGMTFolder += "/";
	   DBManager.connect();
	   //listTables();
	   fetchTerms();
	   System.out.println(termsBP.size() + " GO BP terms were fetched.");
	   System.out.println(termsMF.size() + " GO MF terms were fetched.");
	   System.out.println(termsCC.size() + " GO CC terms were fetched.");
	   
	   long t1 = System.currentTimeMillis();
	   ProgressBar p = new ProgressBar("GO Cellular Component", termsCC.size());
	   for(String goterm:termsCC.keySet())
	   {
		   GOTerm go = termsCC.get(goterm);
		   go.genes = fetchGOGenesDirect(go.id, taxonId);
		   go.descendants = fetchDescendant(go.id);
		   p.increment();
	   }
	   p.close();
	   
	   // TODO add genes of descendants
	   
	   System.out.println("GO CC processing time: "+ Utils.toReadableTime(System.currentTimeMillis() - t1));
   }
   
   public static void generateGODB(String outputGMTFolder, String species) throws IOException
   {
	   int taxonId = 0;
	   if(species.equals("hsa")) taxonId = 9606;
	   if(species.equals("mmu")) taxonId = 10090;
	   if(!outputGMTFolder.endsWith("/")) outputGMTFolder += "/";
	   DBManager.JDBC_DRIVER = "com.mysql.jdbc.Driver";
	   DBManager.URL = "jdbc:mysql://mysql.ebi.ac.uk:4085/go_latest&user=go_select&password=amigo";
	   DBManager.connect();
	   fetchTerms();
	   System.out.println(termsBP.size() + " GO BP terms were fetched.");
	   System.out.println(termsMF.size() + " GO MF terms were fetched.");
	   System.out.println(termsCC.size() + " GO CC terms were fetched.");
	   
	   long t1 = System.currentTimeMillis();
	   ProgressBar p = new ProgressBar("GO Cellular Component", termsCC.size());
	   BufferedWriter bw = new BufferedWriter(new FileWriter(outputGMTFolder + "GO_CC_"+species+".gmt"));
	   for(String goterm:termsCC.keySet())
	   {
		   GOTerm go = termsCC.get(goterm);
		   ArrayList<String> genes = fetchGOGenesRecursively(go.id, taxonId);
		   if(genes.size() > 0) // If the GO is empty then it's useless to print it
		   {
			   bw.write(go.id + "\t" + go.description + "\t" + "http://amigo.geneontology.org/amigo/term/"+go.id);
			   for(String gene:genes) bw.write("\t" + gene);
			   bw.write("\n");
		   }
		   p.increment();
	   }
	   p.close();
	   bw.close();
	   System.out.println("GO CC processing time: "+ Utils.toReadableTime(System.currentTimeMillis() - t1));
	   
	   t1 = System.currentTimeMillis();
	   p = new ProgressBar("GO Molecular Function", termsMF.size());
	   bw = new BufferedWriter(new FileWriter(outputGMTFolder + "GO_MF_"+species+".gmt"));
	   for(String goterm:termsMF.keySet())
	   {
		   GOTerm go = termsMF.get(goterm);
		   ArrayList<String> genes = fetchGOGenesRecursively(go.id, taxonId);
		   if(genes.size() > 0) // If the GO is empty then it's useless to print it
		   {
			   bw.write(go.id + "\t" + go.description + "\t" + "http://amigo.geneontology.org/amigo/term/"+go.id);
			   for(String gene:genes) bw.write("\t" + gene);
			   bw.write("\n");
		   }
		   p.increment();
	   }
	   p.close();
	   bw.close();
	   System.out.println("GO MF processing time: "+ Utils.toReadableTime(System.currentTimeMillis() - t1));
	   
	   t1 = System.currentTimeMillis();
	   p = new ProgressBar("GO Biological Process", termsBP.size());
	   bw = new BufferedWriter(new FileWriter(outputGMTFolder + "GO_BP_"+species+".gmt"));
	   for(String goterm:termsBP.keySet())
	   {
		   GOTerm go = termsBP.get(goterm);
		   ArrayList<String> genes = fetchGOGenesRecursively(go.id, taxonId);
		   if(genes.size() > 0) // If the GO is empty then it's useless to print it
		   {
			   bw.write(go.id + "\t" + go.description + "\t" + "http://amigo.geneontology.org/amigo/term/"+go.id);
			   for(String gene:genes) bw.write("\t" + gene);
			   bw.write("\n");
		   }
		   p.increment();
	   }
	   p.close();
	   bw.close();
	   System.out.println("GO BP processing time: "+ Utils.toReadableTime(System.currentTimeMillis() - t1));
	   
	   DBManager.disconnect();
   }
   
   public static void fetchTerms()
   {
	   termsBP = new HashMap<String, GOTerm>();
	   termsMF = new HashMap<String, GOTerm>();
	   termsCC = new HashMap<String, GOTerm>();
	   Statement stmt = null;
	   try
	   {   
		   	stmt = DBManager.conn.createStatement();
	      
		   	String sql = "SELECT * FROM term";
			ResultSet rs = stmt.executeQuery(sql);
		    while(rs.next())
		    {
		    	GOTerm go = new GOTerm();
		    	if(rs.getInt("is_obsolete") == 0)
		    	{
		    		go.id = rs.getString("acc");
		    		go.description = rs.getString("name");
			    	String term_type = rs.getString("term_type");
			    	if(term_type.equals("biological_process")) termsBP.put(go.id, go);
			    	else if(term_type.equals("molecular_function")) termsMF.put(go.id, go);
			    	else if(term_type.equals("cellular_component")) termsCC.put(go.id, go);
		    	}
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
   }
   
   public static ArrayList<String> fetchGOGenesRecursively(String goId, int taxon)
   {
	   ArrayList<String> genes = new ArrayList<String>();
	   Statement stmt = null;
	   try
	   {   
		   	stmt = DBManager.conn.createStatement();
	      
		   	String sql = ""
		   			+ "SELECT DISTINCT gene_product.symbol FROM term"
		   			+ " INNER JOIN graph_path ON (term.id=graph_path.term1_id)" // For all genes from subtrees
		   			+ " INNER JOIN association ON (graph_path.term2_id=association.term_id)"
		   			+ " INNER JOIN gene_product ON (association.gene_product_id=gene_product.id)"
		   			+ " INNER JOIN species ON (gene_product.species_id=species.id)"
		   			+ " INNER JOIN dbxref ON (gene_product.dbxref_id=dbxref.id)" // For restricting results to Uniprot and not PDB
		   			+ " WHERE term.acc = '"+ goId +"' AND  ncbi_taxa_id = "+ taxon; // dbxref.xref_dbname = 'UniProtKB' AND
		   			//+ " INNER JOIN evidence ON (association.id=evidence.association_id)"
		   			//+ " WHERE evidence.code='IEA' AND dbxref.xref_dbname = 'UniProtKB' AND term.acc = '"+ goId +"' AND  ncbi_taxa_id = "+ taxon;
			ResultSet rs = stmt.executeQuery(sql);
		    while(rs.next()) genes.add(rs.getString("symbol").toUpperCase().trim());
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
	   return genes;
   }
   
   public static ArrayList<String> fetchGOGenesDirect(String goId, int taxon)
   {
	   ArrayList<String> genes = new ArrayList<String>();
	   Statement stmt = null;
	   try
	   {   
		   	stmt = DBManager.conn.createStatement();
	      
		   	String sql = ""
		   			+ "SELECT DISTINCT gene_product.symbol FROM term"
		   			+ " INNER JOIN association ON (term.id=association.term_id)"
		   			+ " INNER JOIN gene_product ON (association.gene_product_id=gene_product.id)"
		   			+ " INNER JOIN species ON (gene_product.species_id=species.id)"
		   			+ " INNER JOIN dbxref ON (gene_product.dbxref_id=dbxref.id)" // For restricting results to Uniprot and not PDB
		   			+ " WHERE dbxref.xref_dbname = 'UniProtKB' AND term.acc = '"+ goId +"' AND  ncbi_taxa_id = "+ taxon;
		   			//+ " INNER JOIN evidence ON (association.id=evidence.association_id)"
		   			//+ " WHERE evidence.code='IEA' AND dbxref.xref_dbname = 'UniProtKB' AND term.acc = '"+ goId +"' AND  ncbi_taxa_id = "+ taxon;
			ResultSet rs = stmt.executeQuery(sql);
		    while(rs.next()) genes.add(rs.getString("gene_product.symbol").toUpperCase().trim());
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
	   return genes;
   }
   
   public static ArrayList<String> fetchDescendant(String goId) // Get nodes, subnodes, subsubnodes, etc...
   {
	   // TODO: eventually get only direct descendants? distance = 1?
	   ArrayList<String> go = new ArrayList<String>();
	   Statement stmt = null;
	   try
	   {   
		   	stmt = DBManager.conn.createStatement();
		   	String sql = ""
		   			+ "SELECT DISTINCT descendant.acc FROM term"
		   			+ " INNER JOIN graph_path ON (term.id=graph_path.term1_id)" // For all genes from subtrees
		   			+ " INNER JOIN term AS descendant ON (descendant.id=graph_path.term2_id)"
		   			+ " WHERE term.acc = '"+ goId +"' AND distance <> 0";
			ResultSet rs = stmt.executeQuery(sql);
		    while(rs.next()) go.add(rs.getString("descendant.acc").trim());
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
	   return go;
   }
}

class GOTerm
{
	String id;
	String description;
	ArrayList<String> genes;
	ArrayList<String> descendants;
}

class ProgressBar
{
	public ProgressMonitor progressBar;
	public int max;
	public int step;
	
	public ProgressBar(String dbname, int max)
	{
		this.max = max;
		this.step = 0;
	    progressBar = new ProgressMonitor(null, "Fetching database "+dbname, null, 0, max);
	}
	
	public void increment()
	{
		step++;
	    progressBar.setProgress(step);
	    progressBar.setNote(step + " GOCC Terms fetched over "+max);
	    if(progressBar.isCanceled()) close();
	}
	
	public void close()
	{
		progressBar.close();
	}
}