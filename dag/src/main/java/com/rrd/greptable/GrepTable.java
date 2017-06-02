package com.rrd.greptable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rrd.dag.DAG;

public class GrepTable {
	
	public static void main(String[] args) throws Exception {
//		if(args ==null || args.length < 1) {
//			System.out.println("please input file: ");
//			System.exit(-1);
//		}
//		String path = args[0];
//		Map<String,Set<String>> map = parseTable(readFile(path));
//		
//		//遍历map,取出所有元素
//		for(Map.Entry<String, Set<String>> entry : map.entrySet()) {
//			// 取出母表
//			String originTable = entry.getKey();
//			System.out.println(originTable);
//			for(String table : entry.getValue()) {
//				System.out.println("  " + table);
//			}
//		}
		
		
		
		Map<String,Set<String>> map = getTotalTableMap("E:/rrd/new_repo/data-warehouse/dw_project");
		DAG dag = new DAG();
		
		//遍历map,取出所有元素
		for(Map.Entry<String, Set<String>> entry : map.entrySet()) {
			// 取出母表
			String originTable = entry.getKey();
			System.out.println(originTable);
			dag.addVertex(originTable);
			for(String table : entry.getValue()) {
				System.out.println("  " + table);
				dag.addEdge(originTable, table);
			}
		}
		
		System.out.println(map.size());
		
//		for(Map.Entry<String,Set<String>> entry : map.entrySet()) {
//			String key = entry.getKey();
//			dag.addVertex(key);
//			for(String table : entry.getValue()) {
//				dag.addEdge(key, table);
//			}
//		}
		
//		System.out.println(dag.toString());
		
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
			sb.append(line.trim()).append(" ");
		}
		br.close();
		return sb.toString();
	}
	
	
	
	/**
	 * 从字符串中，匹配出表，梳理表与表之间关系，并将上游表作为key，下游表列表作为value保存在map中
	 * @param content
	 * @return
	 */
	public static Map<String,Set<String>> parseTable(String content) {
		String regex1 = "insert overwrite table *\\w*\\.(\\w*)";
//		String regex2 = "[F|f][R|r][O|o][M|m] *(fdm|gdm|adm)\\.([\\w|\\d]*)";
		String regex2 = "[F|f][R|r][O|o][M|m] *([\\w|\\d]*)\\.([\\w|\\d]*)";
		Pattern p1 = Pattern.compile(regex1);
		Pattern p2 = Pattern.compile(regex2);
		Matcher m1 = p1.matcher(content);
		Matcher m2 = p2.matcher(content); 
		Map<String, Set<String>> map = new HashMap<>();
		Set<String> list = new TreeSet<String>();
		String key = "";
		if(m1.find()) {
			key = m1.group(1);
		}
		while(m2.find()) {
			list.add(m2.group(2));
		}
		map.put(key, list);
		return map;
	}
	
	/**
	 * 获取指定目录下的所有文件中涉及到的表，并找出这些表所依赖的表（仅一层），作为总的数据
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static Map<String,Set<String>> getTotalTableMap(String path) throws Exception {
		
		final Map<String,Set<String>> all = new HashMap<String,Set<String>>();
		
		final List<File> files = new ArrayList<File>();
		SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>(){
		    @Override
		    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
		        files.add(file.toFile());
		        
				try {
			        // 1. 获取文件绝对路径
					String absPath = file.toAbsolutePath().toString();
		        
					// 2. 读取文件,提取文件中的表关系，并加入到统一的Map中，用于构建整体的DAG图
					all.putAll(parseTable(readFile(absPath)));	
					
				} catch (Exception e) {
					e.printStackTrace();
				}
//		        System.out.println(file.toAbsolutePath().toString());
		        return super.visitFile(file, attrs);
		    }
		};
	    Files.walkFileTree(Paths.get(path), finder);
	    
		return all;
	}
	
	public static Map<String,Set<String>> getOneTableMap(Map<String,Set<String>> total, String tblName) {
		
		Map<String,Set<String>> map = new HashMap<String,Set<String>>();
		
		// 找到该表对应的set
		Set<String> value = total.get(tblName);
		for(String tbl : value) {
			if(total.containsKey(tbl) && total.get(tbl) != null && total.size() > 0) {
				
			}
		}
		
		return map;
	}
}
