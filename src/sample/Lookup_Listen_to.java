package sample;

import java.io.*;
import java.net.*;

public class Lookup_Listen_to implements Runnable{
	
	protected static int L_port = 7055;
	
	public Lookup_Listen_to(int L_port) {
		Lookup_Listen_to.L_port = L_port;
	}
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(L_port);
			while(true){
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
					LookupOps.receive(message);
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

