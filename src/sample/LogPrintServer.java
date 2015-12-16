package sample;

//import net.LogServer;
//import org.ini4j.Wini;

import java.io.BufferedReader;
//import java.io.File;
import java.io.FileReader;
import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;

/**
 * Created by Yongtao on 9/17/2015.
 *
 * Demo log server. It should be started before other clients/servers.
 */
public class LogPrintServer{
	private static String pathname = "/home/groupe/E2_Box/Log/";
	public static void main(String args[]){
		/*try{
			Wini conf=new Wini(new File("conf/sample/sample.ini"));
			int port=conf.get("log","port",int.class);
			new net.LogServer(port);
		}catch(IOException e){
			e.printStackTrace();
		}*/
		
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
