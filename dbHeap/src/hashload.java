import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class hashload {

	// accepts pagesize

	// hash.pagesize
	int bucketSize = 4096;
	int pagesize = 4096;
	int[] primes = { 37, 43, 53, 61, 73 };
	private final String HASH_FNAME = "/Users/asliyoruk/Desktop/heapfile/dbHeap/src/heap.";

	int pageSize;
	
	

	public static void main(String[] args) {

		System.out.println("fdfsdsddsf");
		hashload hashload = new hashload();
		hashload.createHashFile();

	}

	private void createHashFile() {
		List<String> testData = new ArrayList<>();
		testData.add("Zaki");
		testData.add("Asli");
		testData.add("gfhsghsfd");
		testData.add("fsdfd");
		
		
		
		File heapfile = new File(HASH_FNAME + pagesize);
		BufferedReader br = null;
		FileOutputStream fos = null;
		String line = "";
		String nextLine = "";
		String stringDelimeter = "\t";
		byte[] RECORD = new byte[8+100];
		int outCount, pageCount, recCount = 0;

		try {
			fos = new FileOutputStream(heapfile);
			
			for (String test : testData){
				int hash = generateHash(test);
				int bucket = hash % bucketSize;
				String key = String.valueOf(hash);
				String index = "19999";
			      byte[] offset = new byte[8];
			      byte[] keyBytes = key.trim().getBytes();
			       System.arraycopy(keyBytes, 0,
			                RECORD, 0, keyBytes.length);
			       System.arraycopy(index.getBytes(), 0,
			                RECORD, keyBytes.length, index.getBytes().length);
			       
			       fos.write(RECORD);;
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					fos.close();
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private int generateHash(String string) {

		int hash = 23;

		for (int i = 0; i < string.length(); i++) {
			int multiplier = 1;
			if (i < primes.length) {
				multiplier = primes[i];
			}
			hash = hash * 31 + (multiplier * string.charAt(i));
		}

		return hash;
	}

}
