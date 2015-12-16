package sample;

import java.io.*;
import java.net.*;

public class Monitor_Listen_to implements Runnable{
	
	protected static int M_port = 7080;
	
	public Monitor_Listen_to(int M_port){
		Monitor_Listen_to.M_port = M_port;
	}
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(M_port);
			while(true){
				System.out.println("");
				Socket s = ss.accept();
				InputStream is = s.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
				String message = (String) ois.readObject();
				if(message.substring(0, 2).equals("ED")) {
					ois.close();
					is.close();
					s.close();
					break;
				}
				else {
					MonitorOps.receive(message);
					ois.close();
					is.close();
					s.close();
				}
			}
			ss.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}

