import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class hashload implements dbimpl {

	// accepts pagesize

	// hash.pagesize
	int bucketSize = 4096;
	int pagesize = 4096;
	int[] primes = { 37, 43, 53, 61, 73 };
	private final String HASH_FNAME = "hash.";
	private final String HEAP_FNAME = "heap.";

	int pageSize;

	public static void main(String[] args) {

		hashload hashload = new hashload();
		hashload.readAllRecordsInHeap(4096);

	}

	public void readAllRecordsInHeap(int pagesize) {

		File hashfile = new File(HASH_FNAME + pagesize);
		BufferedReader hashReader = null;
		FileOutputStream fosHash = null;
		String line = "";
		String nextLine = "";
		String stringDelimeter = "\t";
		byte[] RECORD = new byte[8 + 100];
		int outCountHash, pageCountHash, recCountHash = 0;

		File heapfile = new File(HEAP_FNAME + pagesize);
		int intSize = 4;
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		int recordNumber = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		try {
			FileInputStream fis = new FileInputStream(heapfile);
			fosHash = new FileOutputStream(hashfile);
			// reading page by page
			while (isNextPage) {
				byte[] bPage = new byte[pagesize];
				byte[] bPageNum = new byte[intSize];
				fis.read(bPage, 0, pagesize);
				System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);

				// reading by record, return true to read the next record
				isNextRecord = true;
				while (isNextRecord) {
					byte[] bRecord = new byte[RECORD_SIZE];
					byte[] bRid = new byte[intSize];
					try {
						System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
						System.arraycopy(bRecord, 0, bRid, 0, intSize);
						rid = ByteBuffer.wrap(bRid).getInt();
						if (rid != recCount) {
							isNextRecord = false;
						} else {
							addToHashFile(bRecord, fosHash, recordNumber++);
							recordLen += RECORD_SIZE;
						}
						recCount++;
						// if recordLen exceeds pagesize, catch this to reset to
						// next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
				}
				// check to complete all pages
				if (ByteBuffer.wrap(bPageNum).getInt() != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fosHash.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void addToHashFile(byte[] bRecord, FileOutputStream fosHash, int rid) throws IOException {
		String bName = new String(bRecord).substring(RID_SIZE + REGISTER_NAME_SIZE,
				RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);


		byte[] RECORD = new byte[32];
		int hash = generateHash(bName);
		int bucket = hash % bucketSize;
		String key = String.valueOf(hash);
		byte[] offset = new byte[16];
		byte[] keyBytes = new byte[16];
		byte[] ridBytes = new byte[16];
		System.arraycopy(key.trim().getBytes(), 0, keyBytes, 0, key.trim().getBytes().length);
		System.arraycopy(keyBytes, 0, RECORD, 0, 16);
		System.arraycopy(String.valueOf(rid).getBytes(), 0, ridBytes, 0, String.valueOf(rid).getBytes().length);
		System.arraycopy(ridBytes, 0, RECORD, 16, 16);
//		System.out.println(new String(RECORD));
		fosHash.write(RECORD);
//		System.out.println(new String(RECORD));
	}

	private void createHashFildsdsde() {
		List<String> testData = new ArrayList<>();
		testData.add("Asli");

		File heapfile = new File(HASH_FNAME + pagesize);
		BufferedReader br = null;
		FileOutputStream fos = null;
		String line = "";
		String nextLine = "";
		String stringDelimeter = "\t";
		byte[] RECORD = new byte[8 + 100];
		int outCount, pageCount, recCount = 0;

		try {
			fos = new FileOutputStream(heapfile);

			for (String test : testData) {
				int hash = generateHash(test);
				int bucket = hash % bucketSize;
				String key = String.valueOf(hash);
				String index = "19999";
				byte[] offset = new byte[8];
				byte[] keyBytes = key.trim().getBytes();
				System.arraycopy(keyBytes, 0, RECORD, 0, keyBytes.length);
				System.arraycopy(index.getBytes(), 0, RECORD, keyBytes.length, index.getBytes().length);

				fos.write(RECORD);
				;
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

	@Override
	public void readArguments(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isInteger(String s) {
		// TODO Auto-generated method stub
		return false;
	}

}
