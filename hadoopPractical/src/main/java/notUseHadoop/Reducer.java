package notUseHadoop;

import java.util.HashMap;

public class Reducer {
	public void reduce(Object[] obj){
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for(Object o : obj){
			HashMap<String, Integer> kv = (HashMap<String, Integer>) o;
			
			for(String k : kv.keySet()){
				if(map.containsKey(k)){
					map.put(k, map.get(k)+kv.get(k));
				}else{
					map.put(k, kv.get(k));
				}
			}
		}
		//the totals number of entries
		int sum =0;
		for (String t : map.keySet()) {
			sum+=map.get(t);
		}
		System.out.println("The totals number of entries is: "+sum);
		//the	number	of	distinct	IPs
		System.out.println("==================");
		System.out.println("The number of distinct IPs is: "+map.size());
		//the	number	of	entries	for	eachIP
		System.out.println("==================");
		System.out.println("The number of entries for eachIP is: ");
		for (String t : map.keySet()) {
			System.out.println(t + " : " + map.get(t));
		}
	}
}
