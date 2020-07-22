package cs455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

public class Util {
	private static BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));	
	
	protected static String normStr(String str) {
		StringBuffer buff 		= new StringBuffer(str);
		for(int i=0; i < buff.length(); i++){
			char c 			= buff.charAt(i);
			if(!Character.isLetter(c) || Character.isDigit(c)){
				buff.deleteCharAt(i);
				i--;
			}
		}
		return buff.toString().trim().toLowerCase();
	}

	protected static String join(String s1, String s2) {
		return s1 + "|" + s2;
	}

	protected static String readSentence(){
		try {
			return bufferedReader.readLine();
		} catch (IOException e) { System.out.println("IO problem: " + e.getMessage()); }
		return  null;
	}

	protected static String[] parseText(String text, char pattern){
		LinkedList<String> parts 	= new LinkedList<String>(); 
		while (true){
			int loc 		= text.indexOf(pattern) ;
			if(loc == -1)
				break;
			parts.add(text.substring(0, loc).trim());
			text 			= text.substring(loc+1).trim();
		}
		parts.add(text.trim());
		return (String[]) parts.toArray(new String[0]);
	}
	
	protected static int contains(String[] stringList,String stringElement){
		for(int i = 0 ; i < stringList.length; ++i)
			if (stringList[i].compareToIgnoreCase(stringElement) == 0)
				return i;
		return -1;
	}
}
