package sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;


public class AdminOps {

	private static int M_port = 7080;
	private static String M_ip = "localhost";
	private static String pathname = "/home/groupe/E2_Box/Admin/";
	public static void main(String[] args) {
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Monitor_Listner_Details.txt"));
			M_ip = br.readLine().trim();
			M_port = Integer.parseInt(br.readLine().trim());
			System.out.println("Monitor ip is " + M_ip + " and Monitor port is " + M_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Scanner scanner;
		while (true) {
			scanner = new Scanner(System.in);
		    System.out.print("Issue Admin Request : ");
		    String AR = scanner.next();
		    if(AR.equals("Exit")) {
		    	break;
		    }
		    else {
		    	System.out.println("Admin request is " + AR);
		    	send_To(AR, M_ip + ":" +  M_port);
		    }
		}
		scanner.close();
	    
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
