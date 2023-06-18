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

public class CleanGeo {
	public static final String NULL = "NULL";
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		int firstLen = -1;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream (new FileInputStream(args[0]))));
		PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args[1])))));
		
		String line = null;
		
		// geo tweet data
		int[] project = new int[]{ 1, 2, 3, 28, 29, 31, 32, 33 };
		
		while((line=br.readLine())!=null){
			String[] cols = line.split("\t");
			if(firstLen<0){
				firstLen = cols.length;
			} else if(cols.length!=firstLen){
				System.err.println("Uneven row ("+firstLen+") :'"+line+"'");
				continue;
			} 
			
			StringBuffer buf = new StringBuffer();
			int nullCount = 0;
			for(int i=0; i<project.length; i++){
				if(i>0) buf.append("\t");
				String val = cols[project[i]].trim();
				
				buf.append(val);
				
				if(val.equals(NULL)){
					nullCount ++;
				}
			}
			
			// if we have any geo data
			if(nullCount!=5){
				pw.println(buf.toString());
			}
		}
		
		br.close();
		pw.close();
	}
}
