package com.rrd.dag;


public class DefaultEdge extends org.jgrapht.graph.DefaultEdge {
	private static final long serialVersionUID = 3258408452177932855L;

	/**
	 * Retrieves the source of this edge. This is protected, for use by
	 * subclasses only (e.g. for implementing toString).
	 *
	 * @return source of this edge
	 */
	protected Object getSource() {
		return getSource();
	}

	/**
	 * Retrieves the target of this edge. This is protected, for use by
	 * subclasses only (e.g. for implementing toString).
	 *
	 * @return target of this edge
	 */
	protected Object getTarget() {
		return getTarget();
	}

	@Override
	public String toString() {
		return "";
	}
}
