import db.DBManager;
import enrichment.Enrichment;
import model.Mode;
import model.Parameters;
import parsing.CreateDLFile;
import parsing.FileParser;
import parsing.RegenerateNewOrganism;
import tools.Utils;

public class ASAP 
{
	public static Mode m = null;

	public static void main(String[] args) 
	{
		DBManager.JDBC_DRIVER = "org.postgresql.Driver";
		DBManager.URL = "jdbc:postgresql://localhost/dbName?user=toto&password=tata";
		
		String[] args2 = readMode(args);
		Parameters.load(args2, m);
		switch(m)
		{
			case CreateEnrichmentDB:
				DBManager.generateEnrichmentDB();
				break;
			case CreateGenesDB:
				DBManager.createDB();
				break;
			case Enrichment:
				Enrichment.readFiles();
				Enrichment.runEnrichment();
				break;
			case Parsing: 
				long t = System.currentTimeMillis();
				FileParser.dbGenes = DBManager.getGenesInDB();
				System.out.println("Accessing DB time: " + Utils.toReadableTime(System.currentTimeMillis() - t));
				t = System.currentTimeMillis();
				FileParser.parse();
				System.out.println("Parsing time: " + Utils.toReadableTime(System.currentTimeMillis() - t));
				break;
			case PopulateDB:
				//DBManager.populateDB();
				break;
			case RegenerateNewOrganism:
				RegenerateNewOrganism.regenerateJSON();
				break;
			case CreateDLFile:
				CreateDLFile.getGenesFromJSON(Parameters.JSONFileName);
				CreateDLFile.create(Parameters.fileName, Parameters.outputFile);
		}
	}
				
	public static String[] readMode(String[] args)
	{
		String[] args2 = null;
		if(args.length >= 2)
		{
			args2 = new String[args.length - 2];
			int j = 0;
			for(int i = 0; i < args.length; i++) 
			{
				String arg = args[i];
				switch(arg)
				{
					case "-T":
						i++;
						String mode = args[i];
						switch(mode)
						{
							case "CreateEnrichmentDB": m = Mode.CreateEnrichmentDB; break;
							case "CreateGenesDB": m = Mode.CreateGenesDB; break;
							case "Enrichment": m = Mode.Enrichment; break;
							case "Parsing": m = Mode.Parsing; break;
							case "PopulateDB": m = Mode.PopulateDB; break;
							case "RegenerateNewOrganism": m = Mode.RegenerateNewOrganism; break;
							case "CreateDLFile": m = Mode.CreateDLFile; break;
							default: System.err.println("Mode (-T) " + mode + " does not exist!"); System.out.println("-T %s \t\tMode to run ASAP [Parsing, RegenerateOutput, CreateEnrichmentDB, CreateGenesDB, PopulateDB, Enrichment]."); System.exit(-1);
						}
						break;
					default:
						args2[j] = arg;
						j++;
				}
			}
		}
		if(m == null || args.length < 2)
		{
			System.out.println("Argument -T is mandatory:");
			System.out.println("-T %s \t\tMode to run ASAP [Parsing, RegenerateOutput, CreateEnrichmentDB, CreateGenesDB, PopulateDB, Enrichment].");
			System.exit(-1);
		}
		return args2;
	}
}
