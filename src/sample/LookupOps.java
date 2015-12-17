package sample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;

public class LookupOps {
	
	private static int lookupcount = 1;
	//private static String pathname = "D:\\ceph/";
	private static String pathname = "/home/groupe/E2_Box/OSD/OSD";
	private static String M_ip = "localhost";
	private static int M_port = 7080, O_port = 7053, L_port;
	private static LinkedList<String> SAVE_msg_queue = new LinkedList<String>();
	private static LinkedList<String> LOOK_msg_queue = new LinkedList<String>();
	private static boolean received_wipe = false;
	
	public static void main(String args[]) {
		
		String selfHostName;
		try {
			selfHostName = InetAddress.getLocalHost().getHostName();
			InetAddress inetAddress = InetAddress.getByName(selfHostName);
			String ipAddress = inetAddress.getHostAddress();
			pathname = pathname + ipAddress.substring(ipAddress.length() - 1, ipAddress.length()) + "/";
		} catch (UnknownHostException e) {
			System.out.println(e);
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "OSD_Listner_Details.txt"));
			br.readLine();
			O_port = Integer.parseInt(br.readLine().trim());
			System.out.println("OSD port " + O_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Monitor_Listner_Details.txt"));
			M_ip = br.readLine().trim();
			M_port = Integer.parseInt(br.readLine().trim());
			System.out.println("Monitor ip is " + M_ip + " and Monitor port is " + M_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		L_port = O_port + 2;
		Call_Listner();
		while(true) {
			System.out.print("");
			while(! SAVE_msg_queue.isEmpty()) {
				String save = SAVE_msg_queue.removeFirst();
				String filename = save.substring(0,save.lastIndexOf("_"));
				String ipconfigs = save.substring(save.lastIndexOf("_") + 1, save.length());
				System.out.println("saving to lookupfile for file " + filename + " ips as " + ipconfigs);
				try {
					String filepath =  pathname + "Lookup_File.txt";
					BufferedWriter WriteFile = new BufferedWriter(new FileWriter(filepath, true));
					WriteFile.write(filename);
					WriteFile.newLine();
					WriteFile.write(ipconfigs);
					WriteFile.newLine();
					WriteFile.close();
					send_To("MA", M_ip + ":" + M_port);
				}
				catch (Exception e) {
					System.out.println(e + "in WriteFile");
				}
			}
			while(! LOOK_msg_queue.isEmpty()) {
				String look = LOOK_msg_queue.removeFirst();
				String c_ipconfig = look.substring(0, look.indexOf("_"));
				String filename = look.substring(look.indexOf("_")+1, look.length());
				String fname = "";
				String new_ipconfigs = "";
				int got = 0;
				try {
					BufferedReader br = new BufferedReader(new FileReader(pathname + "Lookup_File.txt"));
					while ((fname = br.readLine()) != null && got == 0) {
						if(fname.equals(filename)) {
							new_ipconfigs = br.readLine();
							got = 1;
						}
						else {
							br.readLine();
						}
					}
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if(new_ipconfigs.length() != 0) {
					lookupcount = lookupcount + 1;
				}
				System.out.println("looking from lookupfile for file " + filename + " and got ips as " + new_ipconfigs);
				send_To("LOOK_" + new_ipconfigs, c_ipconfig);
			}
			while(received_wipe == true) {
				received_wipe = false;
				try {
					PrintWriter writer = new PrintWriter(pathname + "Lookup_File.txt");
					writer.print("");
					writer.close();
				} catch (FileNotFoundException e) {
					System.out.println(e);
				}
				System.out.println("cleared lookup file");
				send_To("MA", M_ip + ":" + M_port);
			}
			try {
				String filepath =  pathname + "Lookup_Print.txt";
				BufferedWriter WriteFile = new BufferedWriter(new FileWriter(filepath, true));
				WriteFile.write(lookupcount);
				WriteFile.newLine();
				WriteFile.close();
			}
			catch (Exception e) {
				System.out.println(e + "in WriteFile");
			}
		}
	}
	
	public static void receive(String msg) {
		
		String msg_type = msg.substring(0, 4);
		if (msg_type.equals("SAVE")) {
			String lookup_entry = msg.substring(5, msg.length());
			SAVE_msg_queue.addLast(lookup_entry);
		}
		else if(msg_type.equals("LOOK")) {
			String lookup_search = msg.substring(5, msg.length());
			LOOK_msg_queue.addLast(lookup_search);
		}
		else if(msg_type.equals("WIPE")) {
			received_wipe = true;
		}
	}
	
	public static void Call_Listner() {
		
		(new Thread() {
			public void run() {
				new Lookup_Listen_to(L_port).run();
			}
		}).start();
	}
	
	public static void send_To(String msg, String ipconfig) {
		
		try {
			Socket s = new Socket(ipconfig.split(":")[0], Integer.parseInt(ipconfig.split(":")[1]));
			OutputStream os = s.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(msg);
			oos.close();
			os.close();
			s.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
