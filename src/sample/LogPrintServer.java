package sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Yongtao on 9/17/2015.
 *
 * Demo log server. It should be started before other clients/servers.
 */
public class LogPrintServer{
	private static String pathname = "/home/groupe/E2_Box/Log/";
	public static void main(String args[]){
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Log_Listner_Details.txt"));
			br.readLine();
			int port = Integer.parseInt(br.readLine().trim());
			System.out.println(port);
			br.close();
			new net.LogServer(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
