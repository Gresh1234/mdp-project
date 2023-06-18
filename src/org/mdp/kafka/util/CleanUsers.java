package org.mdp.kafka.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CleanUsers {
	public static void main(String[] args) throws FileNotFoundException, IOException{
		int firstLen = -1;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream (new FileInputStream(args[0]))));
		PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args[1])))));
		
		String line = null;
		
		// core tweet data
		int[] project = new int[]{ 1, 4, 5, 6, 8, 9, 10, 11, 12, 16, 17, 18, 19 };
		
		while((line=br.readLine())!=null){
			String[] cols = line.split("\t");
			if(firstLen<0){
				firstLen = cols.length;
			} else if(cols.length!=firstLen){
				System.err.println("Uneven row ("+firstLen+") :'"+line+"'");
				continue;
			} 
			
			for(int i=0; i<project.length; i++){
				if(i>0) pw.print("\t");
				pw.print(cols[project[i]].trim());
			}
			pw.println();
		}
		
		br.close();
		pw.close();
	}
}
