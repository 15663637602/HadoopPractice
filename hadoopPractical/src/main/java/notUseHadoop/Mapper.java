package notUseHadoop;

import java.util.HashMap;
import java.util.regex.Pattern;

public class Mapper {

	Pattern p1 = Pattern.compile("\n");
	Pattern p2 = Pattern.compile(" ");

	public HashMap<String, Integer> map1(StringBuffer sb) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String[] dividedByLine = p1.split(sb, -1);
		for (String eachLine : dividedByLine) {
			String[] ip = p2.split(eachLine, -1);
			if (!(ip[0].isEmpty())) {
				if (map.containsKey(ip[0])) {
					map.put(ip[0], map.get(ip[0]) + 1);
				} else {
					map.put(ip[0], 1);
				}
			}
		}
		return map;
	}

	public HashMap<String, Integer> map2(StringBuffer sb) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String[] dividedByLine = p1.split(sb, -1);
		for (String eachLine : dividedByLine) {
			String[] ip = p2.split(eachLine, -1);
			if (!(ip[0].isEmpty())) {
				if (map.containsKey(ip[0])) {
					map.put(ip[0], map.get(ip[0]) + 1);
				} else {
					map.put(ip[0], 1);
				}
			}
		}
		return map;
	}

	public HashMap<String, Integer> map3(StringBuffer sb) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String[] dividedByLine = p1.split(sb, -1);
		for (String eachLine : dividedByLine) {
			String[] ip = p2.split(eachLine, -1);
			if (!(ip[0].isEmpty())) {
				if (map.containsKey(ip[0])) {
					map.put(ip[0], map.get(ip[0]) + 1);
				} else {
					map.put(ip[0], 1);
				}
			}
		}
		return map;
	}

	public HashMap<String, Integer> map4(StringBuffer sb) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String[] dividedByLine = p1.split(sb, -1);
		for (String eachLine : dividedByLine) {
			String[] ip = p2.split(eachLine, -1);
			if (!(ip[0].isEmpty())) {
				if (map.containsKey(ip[0])) {
					map.put(ip[0], map.get(ip[0]) + 1);
				} else {
					map.put(ip[0], 1);
				}
			}
		}
		return map;
	}

	public HashMap<String, Integer> map5(StringBuffer sb) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String[] dividedByLine = p1.split(sb, -1);
		for (String eachLine : dividedByLine) {
			String[] ip = p2.split(eachLine, -1);
			if (!(ip[0].isEmpty())) {
				if (map.containsKey(ip[0])) {
					map.put(ip[0], map.get(ip[0]) + 1);
				} else {
					map.put(ip[0], 1);
				}
			}
		}
		return map;
	}

}
