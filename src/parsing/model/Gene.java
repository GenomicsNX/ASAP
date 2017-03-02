package parsing.model;

public class Gene 
{
	public int id;
	public String ensembl_id;
	public String name;
	public String alternate_names;
	public int organism_id;
	public String created_at;
	
	@Override
	public String toString() 
	{
		return id + ": " + ensembl_id + "\t" + name;
	}
}
