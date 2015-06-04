

 
 import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FormatGenreHive extends UDF {
	
	public Text evaluate(Text t) {
		Text text = new Text();
		if (t != null) {
			try {
				String genre = (String)t.toString();
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
								sb.append(parts[i]+" vmm130030 :hive");
							}
							else
							{
								sb.append(parts[i]).append(", ");
							}           				          			 
						}
					} 
				}        	
				else{
					sb.append("1) "+genre+" vmm130030 :hive");
				}
				text.set(sb.toString());   
			}
			catch (Exception e) {
				text = new Text(t);
			}
		}
		return text;
	}
}
