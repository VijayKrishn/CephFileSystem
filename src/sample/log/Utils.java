package sample.log;


import util.Log;
import util.ReconnectSocketHandler;
import util.SingleLineFormatter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Handler;

public class Utils{
	public static void connectToLogServer(Log log) throws IOException{
		String logIP = "localhost";
		int logPort = 7052;
		
		String pathname = "/home/groupe/E2_Box/OSD/OSD";
		String selfHostName;
		try {
			selfHostName = InetAddress.getLocalHost().getHostAddress();
			InetAddress inetAddress = InetAddress.getByName(selfHostName);
			String ipAddress = inetAddress.getHostAddress();
			System.out.println("iplastis" + ipAddress.substring(ipAddress.length() - 1, ipAddress.length()));
			pathname = pathname + ipAddress.substring(ipAddress.length() - 1, ipAddress.length()) + "/";
			System.out.println(pathname);
		} catch (UnknownHostException e) {
			System.out.println(e);
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Log_Listner_Details.txt"));
			logIP = br.readLine();
			logPort = Integer.parseInt(br.readLine().trim());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// set remote log server to forward all logs there
		Handler handler=new ReconnectSocketHandler(logIP,logPort);
		handler.setFormatter(new SingleLineFormatter());
		log.getParent().addHandler(handler);
	}
	
//	public static void main(String[] args) {
//		Log log=Log.get();
//		try {
//			connectToLogServer(log);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}
