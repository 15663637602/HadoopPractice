package notUseHadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Scanner;

public class Practice05 {

	public StringBuffer[] readFile(String url) {

		StringBuffer sb1 = new StringBuffer();
		StringBuffer sb2 = new StringBuffer();
		StringBuffer sb3 = new StringBuffer();
		StringBuffer sb4 = new StringBuffer();
		StringBuffer sb5 = new StringBuffer();
		StringBuffer[] sb = new StringBuffer[5];

		try {
			FileReader fr = new FileReader(url);
			Scanner in = new Scanner(fr);
			int n = 1;
			while (in.hasNextLine() && n <= 1000) {
				sb1.append(in.nextLine() + "\n");
				n++;
			}
			sb[0] = sb1;

			while (in.hasNextLine() && n <= 2000) {
				sb2.append(in.nextLine() + "\n");
				n++;
			}
			sb[1] = sb2;

			while (in.hasNextLine() && n <= 3000) {
				sb3.append(in.nextLine() + "\n");
				n++;
			}
			sb[2] = sb3;

			while (in.hasNextLine() && n <= 4000) {
				sb4.append(in.nextLine() + "\n");
				n++;
			}
			sb[3] = sb4;

			while (in.hasNextLine() && n <= 5000) {
				sb5.append(in.nextLine() + "\n");
				n++;
			}
			sb[4] = sb5;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb;
	}

	public static void main(String[] args) {
		Practice05 p = new Practice05();
		String url = "D:" + File.separator + "access_log";
		// Parse file, divide it into several parts
		StringBuffer[] sb = p.readFile(url);
		// assign tasks to mappers(also do the internal grouping work)
		Mapper map = new Mapper();
		HashMap<String, Integer> kv1;
		HashMap<String, Integer> kv2;
		HashMap<String, Integer> kv3;
		HashMap<String, Integer> kv4;
		HashMap<String, Integer> kv5;
		kv1 = map.map1(sb[0]);
		kv2 = map.map2(sb[1]);
		kv3 = map.map3(sb[2]);
		kv4 = map.map4(sb[3]);
		kv5 = map.map5(sb[4]);
		// assign tasks to reducers
		Reducer reduce = new Reducer();
		Object[] obj = new Object[5];
		obj[0] = kv1;
		obj[1] = kv2;
		obj[2] = kv3;
		obj[3] = kv4;
		obj[4] = kv5;

		reduce.reduce(obj);

	}

}
