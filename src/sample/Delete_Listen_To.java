package sample;

import java.io.*;
import java.net.*;

public class Delete_Listen_To implements Runnable{
	
	protected static int D_port = 7056;
	
	public Delete_Listen_To(int D_port){
		Delete_Listen_To.D_port = D_port;
	}
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(D_port);
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
					DeleteOps.receive(message);
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

