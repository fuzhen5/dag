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

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
//import org.jgrapht.graph.DefaultEdge;
import com.rrd.dag.DefaultEdge;

public class GrepTable {
	
	public static void main(String[] args) throws Exception {
		if(args ==null || args.length < 1) {
			System.out.println("please input file: ");
			System.exit(-1);
		}
		String path = args[0];

		DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);
		dag = getDAG(path);
		
		DirectedAcyclicGraph<String, DefaultEdge> part = searchTable(dag,"gdm_user_channel");
		System.out.println(part.edgeSet().toString());
	}
	
	/**
	 * 创建指定表的DAG
	 * @param dag
	 * @param tbl
	 * @return
	 */
	public static DirectedAcyclicGraph<String, DefaultEdge> searchTable(
			DirectedAcyclicGraph<String, DefaultEdge> dag, String tbl) {
		DirectedAcyclicGraph<String, DefaultEdge> tblDag = new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);
		
		//前驱
		extractDag(dag.getAncestors(dag, tbl),tbl,dag,tblDag);
		
		//后继
		extractDag(dag.getDescendants(dag, tbl),tbl,dag,tblDag);
		return tblDag;
	}
	
	/**
	 * 在partDag中创建dag中指定顶点，及其前驱或后继中的边
	 * @param set
	 * @param tbl
	 * @param dag
	 * @param partDag
	 */
	public static void extractDag(Set<String> set, String tbl,
			DirectedAcyclicGraph<String, DefaultEdge> dag, 
			DirectedAcyclicGraph<String, DefaultEdge> partDag) {
		set.add(tbl);
		Object[] arr = set.toArray();
		for(int i = 0; i < arr.length; i++) {
			partDag.addVertex((String) arr[i]);
		}
		
		for(int i = 0; i < arr.length; i++) {
			for(int j = 0; j < arr.length; j++) {
				if(j == i) {
					continue;
				}
				addEdge(dag,partDag,(String)arr[i],(String)arr[j]);
			}
		}
	}
	

	/**
	 * 根据指定的两个定点以及一个DAG，来判断是否存在边，如果 存在 边，则添加到新的DAG中
	 * @param dag       全局DAG
	 * @param partDag   局部DAG
	 * @param sourceVertex  定点
	 * @param targetVertex  定点
	 */
	private static void  addEdge(DirectedAcyclicGraph<String, DefaultEdge> dag, 
			DirectedAcyclicGraph<String, DefaultEdge> partDag,
			String sourceVertex, String targetVertex) {
		if(dag.containsEdge(sourceVertex, targetVertex)) {
			partDag.addEdge(sourceVertex, targetVertex);
		}
	}
	

	/**
	 * 根据文件中提取出的全表Map,构建DAG
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static DirectedAcyclicGraph<String, DefaultEdge> getDAG(String path) throws Exception {
		DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(DefaultEdge.class);
		Map<String,Set<String>> map = getTotalTableMap(path);
		
		//遍历map,取出所有元素
		for(Map.Entry<String, Set<String>> entry : map.entrySet()) {
			
			// 取出母表
			String originTable = entry.getKey();
			if(originTable == null || originTable.equals("")) {
				continue;
			}
//			System.out.println(originTable);
			dag.addVertex(originTable);
			
			for(String table : entry.getValue()) {
				dag.addVertex(table);
				dag.addEdge(table,originTable);
//				System.out.println("-- " + table);
			}
		}
		return dag ;
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
			String  value = m2.group(2);
			if(! key.equalsIgnoreCase(value)) {
				list.add(m2.group(2));
			}
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
	
	/**
	 * 
	 * @param total    全局Map,保存着全局的关系
	 * @param tblName  要查找的表名称
	 * @return         返回要查找表的对应关系
	 */
	public static Map<String,Set<String>> getOneTableMap(Map<String,Set<String>> total, String tblName) {
		
		Map<String,Set<String>> map = new HashMap<String,Set<String>>();
		// 递归停止的条件 
		if(! total.containsKey(tblName) || total.get(tblName) == null || total.size() == 0) {
			return map;
		}

		// 找到该表对应的set
		Set<String> value = total.get(tblName);
		for(String tbl : value) {
			map = getOneTableMap(total,tbl);
		}
		
		return map;
	}
	
}
