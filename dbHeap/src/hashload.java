import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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
		File heapfile = new File(HASH_FNAME + pagesize);
		BufferedReader br = null;
		FileOutputStream fos = null;
		String line = "";
		String nextLine = "";
		String stringDelimeter = "\t";
		// byte[] RECORD = new byte[RECORD_SIZE];
		int outCount, pageCount, recCount = 0;

		try {
			fos = new FileOutputStream(heapfile);
			fos.write(generateHash("Asli").getBytes());
			fos.write(generateHash("Zaki").getBytes());
			fos.write(generateHash("Hamdani").getBytes());
			fos.write(generateHash("Test").getBytes());
			fos.write(generateHash("Testtytytyt").getBytes());
			fos.write(generateHash("Soe Data").getBytes());
			fos.write(generateHash("Okay").getBytes());
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

	private String generateHash(String string) {

		int hash = 23;

		for (int i = 0; i < string.length(); i++) {
			int multiplier = 1;
			if (i < primes.length) {
				multiplier = primes[i];
			}
			hash = hash * 31 + (multiplier * string.charAt(i));
		}

		return String.valueOf(hash%bucketSize);
	}

}
