package sample;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;

public class DeleteOps {
	
	private static LinkedList<String> DL_msg_queue = new LinkedList<String>();
	private static int D_port = 7056;
	private static String pathname = "/home/groupe/E2_Box/OSD/OSD";
	public static void main(String[] args) {
		
		Call_Listner();
		String selfHostName;
		try {
			selfHostName = InetAddress.getLocalHost().getHostName();
			InetAddress inetAddress = InetAddress.getByName(selfHostName);
			String ipAddress = inetAddress.getHostAddress();
			pathname = pathname + ipAddress.substring(ipAddress.length() - 1, ipAddress.length()) + "/Files/";
		} catch (UnknownHostException e) {
			System.out.println(e);
		}
		while(true) {
			System.out.print("");
			while(! DL_msg_queue.isEmpty()) {
				String filename = DL_msg_queue.removeFirst();
				File f = new File(pathname + filename);
				f.delete();
			}
		}
	}
	
	public static void receive(String msg) {
		
		String msg_type = msg.substring(0, 2);
		if (msg_type.equals("DL")) {
			String filename = msg.substring(3, msg.length());
			DL_msg_queue.addLast(filename);
		}
	}	
	
	public static void Call_Listner() {
		
		(new Thread() {
			public void run() {
				new Delete_Listen_To(D_port).run();
			}
		}).start();
	}
}
