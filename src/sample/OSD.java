package sample;

import java.io.Serializable;


public class OSD implements Serializable{
	private static final long serialVersionUID = 1L;
	int id;
	String ip;
	int port;
	String status;
}