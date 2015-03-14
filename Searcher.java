//package MH_candidate_se;

import java.util.Scanner;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.io.*;

public class Searcher {

	private String filePath;
	private String filePathWnoExtension;
	private static final Charset charset = Charset.forName("utf-8");
	private boolean sortFinished=false, sortStarted=false;

	class SortThread implements Runnable {
		public void run() {
			sortStarted=true;
			// create the sorted one
			ExternalSorter.externalSort(filePathWnoExtension,"name");
			sortFinished=true;
			sortStarted=false;
			// get rid of the chunk files
			final File folder = new File(".");
			final File[] files = folder.listFiles( new FilenameFilter() {
			    @Override
			    public boolean accept( final File dir,
			                           final String name ) {
			        return name.matches(filePathWnoExtension + "_chunk.*\\.tsv" );
			    }
			} );
			for ( final File file : files ) {
			    if ( !file.delete() ) {
			        System.err.println( "Can't remove " + file.getAbsolutePath() );
			    }
			}

			System.out.println("\n" + filePath + " sorted into " + filePathWnoExtension + "_sorted.tsv");
			System.out.println("Searching will now use that file for faster lookup\n");
			prompt();
		}
	}

	private void prompt() {
		System.out.print("search> ");
	}

	public Searcher(String filePath) {
		this.filePath = filePath;
		this.filePathWnoExtension = filePath.split("\\.")[0];
	}

	public String search(String query) {
		File f = new File(filePathWnoExtension + "_sorted.tsv"); 
		System.out.print("Looking for a file: " + filePathWnoExtension + "_sorted.tsv... ");

		// detect if the sorted file was finished sorting
		if (f.exists()) {
			if (sortStarted) { // Means weve sorted it in another run, open for error if sort was terminated
				System.out.println("Found, but still sorting");
			} else {
				sortFinished = true;
				System.out.println("Found");
			}
		} else {
			System.out.println("Not found\nSorting file in background... Please do not exit program.");
			new Thread((new SortThread())).start(); // Only runs sort on one thread. Could make optimizations with Runtime.getRuntime().availableProcessors();
		}


		if (sortFinished) {
			try {
				// Binary Search the sorted file
				// m*logn look up (m = line size)
				RandomAccessFile raf = new RandomAccessFile(filePathWnoExtension+"_sorted.tsv", "r");
				long low = 0, high = raf.length()-1;
				String line;
				long startTime, endTime;
				startTime = System.nanoTime();
				while (low <= high) {
					long mid = (low+high)/2;
					raf.seek(mid);

					// move to the next line
					line = raf.readLine();
					if (line != null) {
						line = raf.readLine();
						line = new String(line.getBytes("ISO-8859-1"),charset);
					}
		
					if (line != null && query.equals(line.split("\t")[0])) { // found it
						endTime = System.nanoTime();
						System.out.println("Binary search found ID in: " + (endTime-startTime)/1000000 + " milliseconds");
						raf.close();
						return "ID for " + query + ": " + line.split("\t")[1];
					}
					else if (query.compareTo(line.split("\t")[0]) < 0) high = mid-1; 
					else low = mid + 1;	
				}
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
			
		} else {

			// Linear Search the unsorted file
			System.out.println("Linear searching instead...");
			BufferedReader bf;
			long startTime, endTime;
			try {
				String line;

				bf = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)), 8192);
				startTime = System.nanoTime();
				while ((line = bf.readLine()) != null) {
					if (line.split("\t")[0].equals(query)){
						endTime = System.nanoTime();
						System.out.println("Linear search found ID in: " + (endTime - startTime)/1000000 + " milliseconds"); // converted to milliseconds
						return "ID for " + query + ": " + line.split("\t")[1];	
					} 
					// System.out.println(line.split("\t")[0]);
				}
				bf.close();

			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}

		return "Couldn't find your query, try again. Note: search is case-sensitive";
	}

	public static void main(String[] args) {
		Searcher searcher = new Searcher(args[0]);

		searcher.prompt();
		Scanner scanner = new Scanner(System.in);
		while(scanner.hasNext()) {
			String results = searcher.search(scanner.nextLine());
			System.out.println(results+"\n");
			searcher.prompt();
		}
	}
}
