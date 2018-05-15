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
		Integer index = readIndex(args[0], pagesize);
		if (index == null) {
			// print
		} else {
			readHeapFile(2523756, pagesize);
		}

	}

	private void readHeapFile(Integer index, int pagesize) {
		System.out.println("Reading Heap file for index " + index);
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
			byte[] bRecord = new byte[RECORD_SIZE];
			
			int recPerPage = pagesize / RECORD_SIZE;
			int pageNumber = index / recPerPage;
			int recOnPage = index % recPerPage;
			
			long offset = (pageNumber * pagesize) + (recOnPage * RECORD_SIZE);
			
			System.out.println("---------" + offset);
			
//			offset = offset + (0 * (int)(offset / pagesize));
			System.out.println("-------->>" + offset);
			
			
			fis.getChannel().position(offset);
			fis.read(bRecord, 0, RECORD_SIZE);

			printRecord(bRecord);

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
		byte[] bPage = new byte[32];
		byte[] bPageNum = new byte[intSize];
		byte[] bRecord = new byte[32];
		byte[] bRid = new byte[intSize];
		File hashfile = new File(HASH_FNAME + pagesize);
		int searchHash = generateHash(search);
		int bucket = searchHash % bucketSize;
		String key = String.valueOf(searchHash);
		String bIndex = null;

		System.out.println(searchHash + "___" + bucket + "____" + key);
		boolean isNextPage = true;
		boolean isNextRecord = true;
		try {
			fis = new FileInputStream(hashfile);

			fis.getChannel().position(bucket * 32);
			fis.read(bPage, 0, 32);
			// System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0,
			// intSize);

			System.arraycopy(bPage, 0, bRecord, 0, 32);

			System.out.println("Found.... " + new String(bRecord));

			bIndex = new String(bRecord).substring(16);
			System.out.println("Extracted index.... " + bIndex);

		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}

		return bIndex == null ? null : Integer.valueOf(bIndex.trim());

	}

	public void printRecord(byte[] rec) {
		String record = new String(rec);
		String BN_NAME = record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);
		System.out.println("-=-=-=-=-=-=-=-=-" + BN_NAME);
		System.out.println(record);
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
