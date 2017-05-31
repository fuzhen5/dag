package com.rrd.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rrd.dag.DAG;

public class SQLParser {
	
	public static void main(String[] args) throws Exception {
		if(args ==null || args.length < 1) {
			System.out.println("please input file: ");
			System.exit(-1);
		}
		String path = args[0];
		DAG dag = new DAG();
		Map<String, List<String>> map = parseTable(readFile(path));
		for(Map.Entry<String,List<String>> entry : map.entrySet()) {
			String key = entry.getKey();
			dag.addVertex(key);
			for(String table : entry.getValue()) {
				dag.addEdge(key, table);
			}
		}
		
		System.out.println(dag.toString());
//		System.out.println("z的child -- " + dag.getChildren("B").toString());
		System.out.println("sink -- " + dag.getSinks().toString());
		System.out.println("sources -- " + dag.getSources().toString());
	}
	
	/**
	 * 读取文件，并将文件内容保存到字符串中
	 * @param path
	 */
	public static String readFile(String path) throws Exception {
		BufferedReader br =new BufferedReader(new FileReader(new File(path)));
		String line = "";
		StringBuilder sb =  new StringBuilder();
		while((line = br.readLine()) != null) {
			if(line.startsWith("#"))
				continue;
			sb.append(line.trim()).append("\r\n");
		}
		br.close();
		return sb.toString();
	}
	
	public static Map<String,List<String>> parseTable(String content) {
		String regex1 = "insert overwrite table *\\w*\\.(\\w*)";
		String regex2 = "from (fdm|gdm|adm)\\.(\\w*)";
		Pattern p1 = Pattern.compile(regex1);
		Pattern p2 = Pattern.compile(regex2);
		Matcher m1 = p1.matcher(content);
		Matcher m2 = p2.matcher(content); 
		Map<String, List<String>> map = new HashMap<>();
		List<String> list = new ArrayList<String>();
		String key = "";
		if(m1.find()) {
			System.out.println("| " + m1.group(1));
			key = m1.group(1);
		}
		while(m2.find()) {
			System.out.println("--| " + m2.group(2));
			list.add(m2.group(2));
		}
		map.put(key, list);
		return map;
	}

}
