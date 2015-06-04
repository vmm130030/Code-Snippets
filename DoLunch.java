
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

public class DoLunch {
    
	private static HashMap<String, List<String>> peggyAdjList = null;
	private static HashMap<String, List<String>> samAdjList = null;
	private static HashSet<String> avoidSet = null;
	private static List<String> peggyStart = null;
	private static List<String> samStart = null;
	private static HashSet<String> peggyAllRes = null;
	private static HashSet<String> samAllRes = null;
	private static TreeSet<String> endResult = null;

	public static void main(String[] args) throws IOException {

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		peggyAdjList = new HashMap<>();
		samAdjList = new HashMap<>();
		avoidSet = new HashSet<>();
		peggyStart = new ArrayList<>();
		samStart = new ArrayList<>();
		peggyAllRes = new HashSet<>();
		samAllRes = new HashSet<>();
		endResult = new TreeSet<>();

	     String line;
	     int sec=-1;// sector marker to read input
            while ((line = stdin.readLine())!= null) { 
         
                switch (line) {
                    case "Map:":
                        sec=0;
                        continue;
                    case "Avoid:":
                        sec = 1;
                        continue;
                    case "Peggy:":
                        sec = 2;
                        continue;
                    case "Sam:":
                        sec = 3;
                        continue;
                }
                
                
                if (sec == 0) { // read Map section
                    String[] split = line.split("\\s+");
                    
                    if (peggyAdjList.containsKey(split[0])) {
                        List<String> list = peggyAdjList.get(split[0]);
                        list.add(split[1]);
                    } else {
                        List<String> list = new ArrayList<>();
                        list.add(split[1]);
                        peggyAdjList.put(split[0], list);
                    }
                    
                    if (samAdjList.containsKey(split[1])) {
                        List<String> list = samAdjList.get(split[1]);
                        list.add(split[0]);
                    } else {
                        List<String> list = new ArrayList<>();
                        list.add(split[0]);
                        samAdjList.put(split[1], list);
                    }
                } else if (sec == 1) { // read Avoid section
                    avoidSet.add(line.trim());
                } else if (sec == 2) {// read Peggy section
                    String[] split = line.split("\\s+");
                    peggyStart.addAll(Arrays.asList(split));
                } else if (sec == 3) {// read Sam section
                    String[] split = line.split("\\s+");
                    samStart.addAll(Arrays.asList(split));
                    break;// end of input
                }
            }

         
		for (String ps : peggyStart)
			findReachableDest(ps, peggyAdjList, peggyAllRes);
                 
        
		for (String ss : samStart)
			findReachableDest(ss, samAdjList, samAllRes);

		Iterator<String> peggyAllResItr = peggyAllRes.iterator();
                
		while (peggyAllResItr.hasNext()) {
			String type = (String) peggyAllResItr.next();
			if (samAllRes.contains(type)) {
				endResult.add(type);
			}
		}
                
		System.out.println();
                
                Iterator<String> endResultItr = endResult.iterator();
                
		while (endResultItr.hasNext()) {
			System.out.println(endResultItr.next());
		}
	}

	private static void findReachableDest(String start,
			HashMap<String, List<String>> adjList, HashSet<String> result) {

		if (avoidSet.contains(start)) {
			return;
		}

		result.add(start);
		Queue<String> queue = new LinkedList<>();
		queue.add(start);

		while (!queue.isEmpty()) {
			String head = queue.poll();

			List<String> nei = adjList.get(head);// list of neighbours of current node

			if (nei != null) {
				for (String n : nei) {
					if (!avoidSet.contains(n) && !result.contains(n)) { // checking whether result exits to optimize solution
						result.add(n);
						queue.add(n);
					}
				}
			}
		}
	}
}
