package model;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class OutputJSON
{
	public int nber_cells = 0;
	public int nber_genes = 0;
	public int nber_not_found_genes = 0;
	public int nber_ercc = 0;
	public boolean is_count_table = true;
	public long nber_zeros = 0;
	public int nber_duplicated_genes = 0;
	public int nber_unique_genes = 0;
	public int nber_all_duplicated_genes = 0;

    public void writeOutputJSON()
    {
    	try
    	{
        	BufferedWriter bw = new BufferedWriter(new FileWriter(Parameters.outputFolder + "output.json"));
        	bw.write("{\"nber_genes\":" + nber_genes + ",");
        	bw.write("\"nber_cells\":" + nber_cells + ",");
        	bw.write("\"nber_not_found_genes\":" + nber_not_found_genes + ",");
        	bw.write("\"nber_duplicated_genes\":" + nber_duplicated_genes + ",");
        	bw.write("\"nber_all_duplicated_genes\":" + nber_all_duplicated_genes + ",");
        	bw.write("\"nber_zeros\":" + nber_zeros + ",");
        	bw.write("\"nber_ercc\":" + nber_ercc + ",");
        	bw.write("\"nber_unique_genes\":" + nber_unique_genes + ",");
        	bw.write("\"is_count_table\":" + (is_count_table?1:0) + "}");
        	bw.close();
    	}
    	catch(IOException ioe)
    	{
    		System.err.println(ioe.getMessage());
    		System.exit(-1);
    	}
    }
    
    public static OutputJSON loadOutputJSON()
    {
    	OutputJSON res = null;
		try
		{
			Gson gson = new GsonBuilder().registerTypeAdapter(Boolean.class, booleanAsIntAdapter).registerTypeAdapter(boolean.class, booleanAsIntAdapter).create();
			JsonReader reader = new JsonReader(new FileReader(Parameters.outputFolder + "output.json"));
			res = gson.fromJson(reader, OutputJSON.class); // contains the whole infos
			reader.close();
		}
		catch(FileNotFoundException nfe)
		{
			System.err.println("The JSON gene list was not found at the given path: " + Parameters.outputFolder + "output.json" + "\nStopping program...");
			System.exit(-1);
		}
		catch(Exception e)
		{
			System.out.println(e);
			System.err.println("Problem detected when reading the JSON gene list. Stopping program...");
			System.exit(-1);
		}
		return res;
    }
    
    private static final TypeAdapter<Boolean> booleanAsIntAdapter = new TypeAdapter<Boolean>() // This is only used to convert JSON int into booleans
    {
    	public void write(JsonWriter out, Boolean value) throws IOException 
    	{
    		if (value == null) out.nullValue();
    		else out.value(value);
    	}
    	  
    	public Boolean read(JsonReader in) throws IOException 
    	{
    		JsonToken peek = in.peek();
    		switch (peek) 
    		{
    			case BOOLEAN: return in.nextBoolean();
    			case NULL: in.nextNull(); return null;
    			case NUMBER: return in.nextInt() != 0;
    			case STRING: return Boolean.parseBoolean(in.nextString());
    			default: throw new IllegalStateException("Expected BOOLEAN or NUMBER but was " + peek);
    		}
    	}
	};
}
