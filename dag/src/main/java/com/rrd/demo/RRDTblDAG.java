package com.rrd.demo;

import java.awt.*;

import javax.swing.*;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.ext.*;
//import org.jgrapht.graph.*;

import com.mxgraph.layout.*;
import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.swing.*;
import com.rrd.greptable.GrepTable;
import com.rrd.dag.DefaultEdge;

public class RRDTblDAG extends JApplet {
	private static final long serialVersionUID = 2202072534703043194L;
	private static final Dimension DEFAULT_SIZE = new Dimension(800, 600);

	private static String path = "";
	private static String tableName = "";

	private JGraphXAdapter<String, DefaultEdge> jgxAdapter;

	/**
	 * An alternative starting point for this demo, to also allow running this
	 * applet as an application.
	 *
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] args) {

		RRDTblDAG applet = new RRDTblDAG();

		if(args ==null || args.length != 2) {
			System.out.println("please check your input ");
			System.exit(-1);
		}
		
		path = args[0];
		tableName = args[1];

		applet.init();

		JFrame frame = new JFrame();
		frame.getContentPane().add(applet);
		frame.setTitle("RRD Data Warehouse Table Relation: ");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.pack();
		frame.setSize(800, 600);
		frame.setVisible(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init() {
		
		// create a JGraphT graph
		DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(
				DefaultEdge.class);
		DirectedAcyclicGraph<String, DefaultEdge> part = new DirectedAcyclicGraph<String, DefaultEdge>(
				DefaultEdge.class);
		try {
			dag = GrepTable.getDAG(path);
			part = GrepTable.searchTable(dag,tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// create a visualization using JGraph, via an adapter
		jgxAdapter = new JGraphXAdapter<>(part);

		getContentPane().add(new mxGraphComponent(jgxAdapter));
		resize(DEFAULT_SIZE);

		// positioning via jgraphx layouts
//		mxCircleLayout layout = new mxCircleLayout(jgxAdapter);
		mxGraphLayout layout = new mxHierarchicalLayout(jgxAdapter);
		layout.execute(jgxAdapter.getDefaultParent());
	}
}
