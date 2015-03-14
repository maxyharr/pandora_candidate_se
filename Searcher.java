import java.util.Scanner;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.io.*;

public class Searcher {

	private String filePath;
	private static final Charset charset = Charset.forName("utf-8");
	private boolean sortFinished=false;

	class SortThread implements Runnable {
		public void run() {
			// create the sorted one
			externalSort(filePath.split("\\.")[0],"name");
			sortFinished=true;
			// get rid of the chunk files
			final File folder = new File(".");
			final File[] files = folder.listFiles( new FilenameFilter() {
			    @Override
			    public boolean accept( final File dir,
			                           final String name ) {
			        return name.matches(filePath.split("\\.")[0] + "_chunk.*\\.tsv" );
			    }
			} );
			for ( final File file : files ) {
			    if ( !file.delete() ) {
			        System.err.println( "Can't remove " + file.getAbsolutePath() );
			    }
			}

			System.out.println("\n" + filePath + " sorted into " + filePath.split("\\.")[0] + "_sorted.tsv");
			System.out.println("Searching will now use that file for faster lookup");
			prompt();
		}
	}

	private void prompt() {
		System.out.print("search> ");
	}

	public Searcher(String filePath) {
		this.filePath = filePath;
	}

	public String search(String query) {
		File f = new File(filePath.split("\\.")[0] + "_sorted.tsv"); 
		System.out.print("Looking for a file named: " + filePath.split("\\.")[0] + "_sorted.tsv... ");

		if(!f.exists()) {
			System.out.println("Not found.\nSorting file in background... Please do not exit program.");
			new Thread((new SortThread())).start();
		}

		// detect if the sorted file was finished sorting, 10% room for error
		File g = new File(filePath);
		if (f.exists() && g.exists() && Math.abs((float) g.length()/f.length()) > 0.9) {
			sortFinished = true;
			System.out.println("Found");
		}

		if (sortFinished) {
			try {
				// Binary Search the sorted file
				// m*logn look up (m = line size)
				RandomAccessFile raf = new RandomAccessFile(filePath.split("\\.")[0]+"_sorted.tsv", "r");
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
						System.out.println("Binary search found id in: " + (endTime-startTime)/1000000 + " milliseconds");
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

			BufferedReader bf;
			Scanner sc;
			long startTime, endTime;
			try {
				String line;

				// ###############################
				// ####    BUFFERED READER    ####
				// ###############################

				bf = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)), 64000);
				startTime = System.nanoTime();
				while ((line = bf.readLine()) != null) {
					if (line.split("\t")[0].equals(query)){
						endTime = System.nanoTime();
						System.out.println("Linear search found id in: " + (endTime - startTime)/1000000 + " milliseconds"); // converted to milliseconds
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
			System.out.println(results);
			searcher.prompt();
		}
	}












	private static void externalSort(String relation, String attribute)
	{
	     try
	     {
	         FileReader intialRelationInput = new FileReader(relation + ".tsv"); 
	         BufferedReader initRelationReader = new BufferedReader(intialRelationInput);
	         String [] header = initRelationReader.readLine().split("\t");
	         String [] row = header;
	         int indexToCompare = getIndexForColumn(header,attribute);
	         ArrayList<String[]> tenKRows = new ArrayList<String[]>();
	                     
	         int numFiles = 0;
	         while (row!=null)
	         {
	             // get 10k rows
	             for(int i=0; i<10000; i++)
	             {
	                 String line = initRelationReader.readLine();
	                 if (line==null) 
	                 {
	                     row = null;
	                     break;
	                 }
	                 row = line.split("\t");
	                 tenKRows.add(row);
	             }
	             // sort the rows
	             tenKRows = mergeSort(tenKRows, indexToCompare);
	             
	             // write to disk
	             FileWriter fw = new FileWriter(relation + "_chunk" + numFiles + ".tsv");
	             BufferedWriter bw = new BufferedWriter(fw);
	             bw.write(flattenArray(header,"\t")+"\n");
	             for(int i=0; i<tenKRows.size(); i++)
	             {
	                 bw.append(flattenArray(tenKRows.get(i),"\t")+"\n");
	             }
	             bw.close();
	             numFiles++;
	             tenKRows.clear();
	         }
	     
	         mergeFiles(relation, numFiles, indexToCompare);
	         
	         initRelationReader.close();
	         intialRelationInput.close();
	         
	     }
	     catch (Exception ex)
	     {
	         ex.printStackTrace();
	         System.exit(-1);
	     }
	}


	private static void mergeFiles(String relation, int numFiles, int compareIndex)
	{
	     try
	     {
	         ArrayList<FileReader> mergefr = new ArrayList<FileReader>();
	         ArrayList<BufferedReader> mergefbr = new ArrayList<BufferedReader>();
	         ArrayList<String[]> filerows = new ArrayList<String[]>(); 
	         FileWriter fw = new FileWriter(relation + "_sorted.tsv");
	         BufferedWriter bw = new BufferedWriter(fw);
	         String [] header;
	             
	         boolean someFileStillHasRows = false;
	         
	         for (int i=0; i<numFiles; i++)
	         {
	             mergefr.add(new FileReader(relation+"_chunk"+i+".tsv"));
	             mergefbr.add(new BufferedReader(mergefr.get(i)));
	             // get each one past the header
	             header = mergefbr.get(i).readLine().split("\t");
	                             
	             if (i==0) bw.write(flattenArray(header,"\t")+"\n");
	             
	             // get the first row
	             String line = mergefbr.get(i).readLine();
	             if (line != null)
	             {
	                 filerows.add(line.split("\t"));
	                 someFileStillHasRows = true;
	             }
	             else 
	             {
	                 filerows.add(null);
	             }
	                 
	         }
	         
	         String[] row;
	         int cnt = 0;
	         while (someFileStillHasRows)
	         {
	             String min;
	             int minIndex = 0;
	             
	             row = filerows.get(0);
	             if (row!=null) {
	                 min = row[compareIndex];
	                 minIndex = 0;
	             }
	             else {
	                 min = null;
	                 minIndex = -1;
	             }
	             
	             // check which one is min
	             for(int i=1; i<filerows.size(); i++)
	             {
	                 row = filerows.get(i);
	                 if (min!=null) {
	                     
	                     if(row!=null && row[compareIndex].compareTo(min) < 0)
	                     {
	                         minIndex = i;
	                         min = filerows.get(i)[compareIndex];
	                     }
	                 }
	                 else
	                 {
	                     if(row!=null)
	                     {
	                         min = row[compareIndex];
	                         minIndex = i;
	                     }
	                 }
	             }
	             
	             if (minIndex < 0) {
	                 someFileStillHasRows=false;
	             }
	             else
	             {
	                 // write to the sorted file
	                 bw.append(flattenArray(filerows.get(minIndex),"\t")+"\n");
	                 
	                 // get another row from the file that had the min
	                 String line = mergefbr.get(minIndex).readLine();
	                 if (line != null)
	                 {
	                     filerows.set(minIndex,line.split("\t"));
	                 }
	                 else 
	                 {
	                     filerows.set(minIndex,null);
	                 }
	             }                                 
	             // check if one still has rows
	             for(int i=0; i<filerows.size(); i++)
	             {
	                 
	                 someFileStillHasRows = false;
	                 if(filerows.get(i)!=null) 
	                 {
	                     if (minIndex < 0) 
	                     {
	                         System.out.println("mindex lt 0 and found row not null" + flattenArray(filerows.get(i)," "));
	                         System.exit(-1);
	                     }
	                     someFileStillHasRows = true;
	                     break;
	                 }
	             }
	             
	             // check the actual files one more time
	             if (!someFileStillHasRows)
	             {
	                 
	                 //write the last one not covered above
	                 for(int i=0; i<filerows.size(); i++)
	                 {
	                     if (filerows.get(i) == null)
	                     {
	                         String line = mergefbr.get(i).readLine();
	                         if (line!=null) 
	                         {
	                             
	                             someFileStillHasRows=true;
	                             filerows.set(i,line.split("\t"));
	                         }
	                     }
	                             
	                 }
	             }
	                 
	         }
	         
	         
	         
	         // close all the files
	         bw.close();
	         fw.close();
	         for(int i=0; i<mergefbr.size(); i++)
	             mergefbr.get(i).close();
	         for(int i=0; i<mergefr.size(); i++)
	             mergefr.get(i).close();
	         
	         
	         
	     }
	     catch (Exception ex)
	     {
	         ex.printStackTrace();
	         System.exit(-1);
	     }
	}

	private static String flattenArray(String[] a, String separator)
	{
		String result = "";
		for(int i=0; i<a.length; i++)
		result+=a[i] + separator;
		return result;
	}

	private static int getIndexForColumn(String [] arr, String val)
	{
	    int result = -1;
	    for(int i=0; i<arr.length; i++) 
	    {
	       if (val.equals(arr[i])) { result=i; break; }
	    }
	    return result;
	}


	// sort an arrayList of arrays based on the ith column
	private static ArrayList<String[]> mergeSort(ArrayList<String[]> arr, int index)
	{
	     ArrayList<String[]> left = new ArrayList<String[]>();
	     ArrayList<String[]> right = new ArrayList<String[]>();
	     if(arr.size()<=1)
	         return arr;
	     else
	     {
	         int middle = arr.size()/2;
	         for (int i = 0; i<middle; i++)
	             left.add(arr.get(i));
	         for (int j = middle; j<arr.size(); j++)
	             right.add(arr.get(j));
	         left = mergeSort(left, index);
	         right = mergeSort(right, index);
	         return merge(left, right, index);
	         
	     }
	     
	}

	// merge the the results for mergeSort back together 
	private static ArrayList<String[]> merge(ArrayList<String[]> left, ArrayList<String[]> right, int index)
	{
	     ArrayList<String[]> result = new ArrayList<String[]>();
	     while (left.size() > 0 && right.size() > 0)
	     {
	         if(left.get(0)[index].compareTo(right.get(0)[index]) < 1)
	         {
	             result.add(left.get(0));
	             left.remove(0);
	         }
	         else
	         {
	             result.add(right.get(0));
	             right.remove(0);
	         }
	     }
	     if (left.size()>0) 
	     {
	         for(int i=0; i<left.size(); i++)
	             result.add(left.get(i));
	     }
	     if (right.size()>0) 
	     {
	         for(int i=0; i<right.size(); i++)
	             result.add(right.get(i));
	     }
	     return result;
	}

}
