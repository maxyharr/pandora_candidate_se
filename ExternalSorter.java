/*
	The code below is code found at:
	http://www.codeodor.com/index.cfm/2007/5/14/Re-Sorting-really-BIG-files---the-Java-source-code/1208
	and modified by Max Harris to work with .tsv files containing Strings. 
	The original code was for sorting .csv files containing integers.
*/
//package MH_candidate_se;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class ExternalSorter {

	public static void externalSort(String relation, String attribute)
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