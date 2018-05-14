import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class hashquery implements dbimpl {

	int bucketSize = 4096;
	int pagesize = 4096;
	int[] primes = { 37, 43, 53, 61, 73 };
	private final String HASH_FNAME = "hash.";
	private final String HEAP_FNAME = "heap.";

	int pageSize;
	private FileInputStream fis;

	public static void main(String args[]) {
		System.out.println("Starting process");
		hashquery load = new hashquery();

		// calculate query time
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();

		System.out.println("Query time: " + (endTime - startTime) + "ms");
	}

	@Override
	public void readArguments(String[] args) {
		readIndex(args[0], pagesize);

	}

	private void readHeapFile(Integer index, int pagesize) {
		File heapfile = new File(HEAP_FNAME + pagesize);
		int intSize = 4;
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		try {
			FileInputStream fis = new FileInputStream(heapfile);
			// reading page by page
			byte[] bPage = new byte[pagesize];
			byte[] bPageNum = new byte[intSize];
			fis.getChannel().position(index);
			fis.read(bPage, 0, pagesize);
			System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);

			byte[] bRecord = new byte[RECORD_SIZE];
			byte[] bRid = new byte[intSize];
			System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
			System.arraycopy(bRecord, 0, bRid, 0, intSize);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	private Integer readIndex(String search, int pagesize) {
		
		System.out.println("Starting to read index");
		
		int intSize = 4;
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		byte[] bPage = new byte[pagesize];
		byte[] bPageNum = new byte[intSize];
		byte[] bRecord = new byte[RECORD_SIZE];
		byte[] bRid = new byte[intSize];
		File hashfile = new File(HASH_FNAME + pagesize);
		int searchHash = generateHash(search);
		int bucket = searchHash % bucketSize;
		String key = String.valueOf(searchHash);

		System.out.println(searchHash + "___" + bucket + "____" + key);
		boolean isNextPage = true;
		boolean isNextRecord = true;
		try {
			fis = new FileInputStream(hashfile);

			fis.read(bPage, 0, pagesize);
			System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);

			System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
			System.arraycopy(bRecord, 0, bRid, 0, intSize);
			
			System.out.println("Found.... " + new String(bRecord));

		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Integer.parseInt(new String(bRecord));

	}

	@Override
	public boolean isInteger(String s) {
		// TODO Auto-generated method stub
		return false;
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

	// accepts querytext pagesize
}
