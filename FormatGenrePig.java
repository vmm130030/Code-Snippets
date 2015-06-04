
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class FormatGenrePig extends EvalFunc<String> {

	public String exec(Tuple input) {
		try {
			if (input == null || input.size() == 0) {
				return null;
			}

			String genre = (String)input.get(0);
			StringBuilder sb=new StringBuilder();

			if (genre.contains("|"))
			{
				String[] parts = genre.split("\\|");       	    

				for(int i=0;i<parts.length;i++)
				{
					sb.append(i+1).append(") ");
					if(i == parts.length-2)
					{           			
						sb.append(parts[i]).append(" & ");           	
					}
					else
					{
						if(i == parts.length-1)
						{
							sb.append(parts[i]+" vmm130030");
						}
						else{
							sb.append(parts[i]).append(", ");
						}
					}           		
				}
			}        	
			else{
				sb.append("1) ").append(genre).append(" vmm130030");
			}
			return sb.toString();
		} 
		catch (Exception ex) {
			System.out.println("Error: " + ex.toString());
		}
		return null;
	}

}
