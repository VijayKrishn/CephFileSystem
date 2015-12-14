package sample;

import java.io.Serializable;

public class Node implements Serializable {
	
	private static final long serialVersionUID = 1L;
	int uniqueid;
	int function;
	double val;
	Node left;
	Node right;
	OSD name;
	
	@Override
	public String toString(){
		return this.uniqueid+"";
	}
}