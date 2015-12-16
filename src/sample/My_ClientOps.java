package sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import net.Address;
import net.IOControl;
import net.Session;
import util.FileHelper;
import util.Log;

import com.google.gson.Gson;

public class My_ClientOps {
	
	private static IOControl control=new IOControl();
	private static final Log log=Log.get();
	private static final long MAX_VALUE = 0xFFFFFFFFL;
	private static String pathname = "/home/groupe/E2_Box/Client/";
	
	public static void main(String[] args) {
		Node map = null;
		Gson gson = new Gson();
		
		String mapfile = "clustermap.json";
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + mapfile));
			map = gson.fromJson(br, Node.class);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String request_on = "";
		File folder = new File(pathname + "Input/");
		File[] listOfFiles = folder.listFiles();
		String list_request_on[] = new String[listOfFiles.length];
		for (int i = 0; i < listOfFiles.length; i++) {
			list_request_on[i] = listOfFiles[i].getName();
		}
		for(int i = 0; i < list_request_on.length; i++) {
			request_on = list_request_on[i];
			String res[] = selectInMap(map, request_on);
			System.out.println("write " + request_on + " on " + res[0] + " " +res[1]);
			my_write(request_on, res);
			try {
				Thread.sleep(20);
			}
			catch (Exception e) {
				e.printStackTrace();
				System.out.println(e);
			}
		}
		System.out.println("done with file spreading to the cluster");
	}
	
	private static void my_write(String request_on, String[] res) {
		String path_osd = "/home/groupe/E2_Box/Client/Input/";
		try{
			for (int i = 0; i < res.length; i++) {
				res[i] = res[i].split(":")[0] + ":" +(Integer.parseInt(res[i].split(":")[1]) + 1);
			}
			ArrayList<Address> addresses = splitAddress(res,0);
			if(upload(control, path_osd + request_on, addresses)) {
				log.i("File upload success.");
				System.out.println("File upload success.");
			}
			else {
				log.i("File upload fails.");
				System.out.println("File upload fails.");
			}
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
		}
	}
	
	static long timeout=60*1000;    //  60 seconds
	
	static ArrayList<Address> splitAddress(String[] tokens,int start){
		ArrayList<Address> result=new ArrayList<>();
		for(int i=start;i<tokens.length;++i){
			String[] parts=tokens[i].split(":");
			if(parts.length!=2) return null;
			try{
				int port=Integer.parseInt(parts[1]);
				Address address=new Address(parts[0],port);
				result.add(address);
			}catch(NumberFormatException e){e.printStackTrace();return null;}
		}
		return result;
	}
	
	static boolean upload(IOControl control,String path,ArrayList<Address> addresses,long position){
		try{
			File file=new File(path);
			FileInputStream fis=new FileInputStream(file);
			FileChannel src=fis.getChannel();
			Session req=new Session(FileWriteMsgType.WRITE_CHUNK);
			String id=file.getName();
			long size=file.length();
			req.set("id",id);
			req.set("size",size);
			req.set("timeout",timeout);
			req.set("address",addresses);
			if(position>0)
				req.set("position",position);
			control.send(req,addresses.get(0));
			SocketChannel dest=req.getSocketChannel();
			FileHelper.upload(src,dest,size);
			fis.close();
			Session result=control.get(req);
			return result.getType()==FileWriteMsgType.WRITE_OK;
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
			return false;
		}
	}
	
	static boolean upload(IOControl control,String path,ArrayList<Address> addresses){
		return upload(control,path,addresses,0);
	}
	
public static String[] selectInMap(Node clustermap, String fileName) {
		
		String FileName = "PlacementRules.txt";
		String str = "";
		int val = 0;
		int rin_row = 0;
		int rin_cabinet = 0;
		int rin_osd = 0;
		try {
			BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + FileName));
			while ((str = ReadFile.readLine()) != null) {
				System.out.print("");
				if (val == 0) {
					rin_row = Character.getNumericValue(str.charAt(0));
					;
					val++;
				} else if (val == 1) {
					rin_cabinet = Character.getNumericValue(str.charAt(0));
					;
					val++;
				} else {
					rin_osd = Character.getNumericValue(str.charAt(0));
					;
				}
			}
			ReadFile.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex);
		}
		String ips_ports[] = new String[rin_row * rin_cabinet *rin_osd];
		int sel_row[] = new int[rin_row];
		int sel_cabinet[] = new int[rin_cabinet * rin_row];
		int sel_osd[] = new int[rin_osd * rin_cabinet * rin_row];
		int til_row = rin_row;
		int pos_row = 0;
		for (int i = 0; i < til_row; i++) {
			double h1;
			String uname;
			int res = 1;
			Node traversemap = clustermap;
			while (traversemap.function == 1) {
				System.out.print("");
				uname = traversemap.uname;
				h1 = hashMix(fileName,i,uname);
				if (traversemap.val != 1) {
					if (h1 > traversemap.val) {
						traversemap = traversemap.right;
						res++;
					} else {
						traversemap = traversemap.left;
					}
				}
				else {
					traversemap = traversemap.left;
				}
			}
			int got = 0;
			for (int j = 0; j < pos_row; j++) {
				if (sel_row[j] == res) {
					got = 1;
				}
			}
			if (got == 1) {
				til_row++;
			} else {
				sel_row[pos_row] = res;
				pos_row++;
			}
		}
		/*System.out.println("rows selected");
		for (int a = 0; a < rin_row; a++) {
			System.out.print("  " + sel_row[a]);
		}
		System.out.println(" ");*/
		int pos_cabinet = 0;
		int checkfromhere1 = 0;
		for (int m = 0; m < rin_row; m++) {
			Node traversemap1 = clustermap;
			for (int n = 1; n < sel_row[m]; n++) {
				traversemap1 = traversemap1.right;
			}
			if (traversemap1.function == 1) {
				traversemap1 = traversemap1.left;
			}
			int til_cabinet = rin_cabinet;
			for (int i = 0; i < til_cabinet; i++) {
				double h2;
				String uname;
				int res = 1;
				Node traversemap2 = traversemap1;
				while (traversemap2.function == 2) {
					System.out.print("");
					uname = traversemap2.uname;
					h2 = hashMix(fileName,i,uname);
					if (traversemap2.val != 1) {
						if (h2 > traversemap2.val) {
							traversemap2 = traversemap2.right;
							res++;
						} else {
							traversemap2 = traversemap2.left;
						}
					}
					else {
						traversemap2 = traversemap2.left;
					}
				}
				int got = 0;
				for (int j = checkfromhere1; j < pos_cabinet; j++) {
					if (sel_cabinet[j] == res) {
						got = 1;
					}
				}
				if (got == 1) {
					til_cabinet++;
				} else {
					sel_cabinet[pos_cabinet] = res;
					pos_cabinet++;
				}
			}
			checkfromhere1 = pos_cabinet;
		}
		/*System.out.println("cabinet selected");
		for (int a = 0; a < rin_cabinet * rin_row; a++) {
			System.out.print("  " + sel_cabinet[a]);
		}
		System.out.println("  ");*/
		int pos_osd = 0;
		int start1 = 0;
		int end1 = rin_cabinet;
		for (int m = 0; m < rin_row; m++) {
			Node traversemap1 = clustermap;
			for (int n = 1; n < sel_row[m]; n++) {
				traversemap1 = traversemap1.right;
			}
			if (traversemap1.function == 1) {
				traversemap1 = traversemap1.left;
			}
			for (int a = start1; a < end1; a++) {
				Node traversemap2 = traversemap1;
				for (int b = 1; b < sel_cabinet[a]; b++) {
					traversemap2 = traversemap2.right;
				}
				if (traversemap2.function == 2) {
					traversemap2 = traversemap2.left;
				}
				int checkfromhere = 0;
				int til_osd = rin_osd;
				for (int i = 0; i < til_osd; i++) {
					double h3;
					String uname;
					int res = 0;
					String ip_port = "";
					String status;
					Node traversemap3 = traversemap2;
					while (true) {
						System.out.print("");
						uname = traversemap3.uname;
						h3 = hashMix(fileName,i,uname);
						if (traversemap3.val != 1) {
							if (h3 > traversemap3.val) {
								traversemap3 = traversemap3.right;
							} else {
								res = traversemap3.name.id;
								String ip1 = traversemap3.name.ip;
								int port1 = traversemap3.name.port;
								status = traversemap3.name.status;
								ip_port = ip1 + ":" + port1;
								break;
							}
						} else {
							res = traversemap3.name.id;
							String ip1 = traversemap3.name.ip;
							int port1 = traversemap3.name.port;
							status = traversemap3.name.status;
							ip_port = ip1 + ":" + port1;
							break;
						}
					}
					if (status.equals("F") || status.equals("O")) {
						til_osd++;
					}
					else {
						int got = 0;
						for (int j = checkfromhere; j < pos_osd; j++) {
							if (sel_osd[j] == res) {
								got = 1;
							}
						}
						if (got == 1) {
							til_osd++;
						} else {
							sel_osd[pos_osd] = res;
							ips_ports[pos_osd] = ip_port;
							pos_osd++;
						}
					}
				}
				checkfromhere = pos_osd;
			}
			start1 = start1 + rin_cabinet;
			end1 = end1 + rin_cabinet;

		}
		/*System.out.println("osd selected");
		for (int a = 0; a < rin_osd * rin_cabinet * rin_row; a++) {
			System.out.print("  " + sel_osd[a]);
		}
		System.out.println("  ");*/
		return ips_ports;
	}
	
	private static long subtract(long val, long subtract) {
		
		return (val - subtract) & MAX_VALUE;
	}

	private static long xor(long val, long xor) {
		
		return (val ^ xor) & MAX_VALUE;
	}

	private static long leftShift(long val, int shift) {
		
		return (val << shift) & MAX_VALUE;
	}

	private static double hashMix(String fileName, int r, String uname) {
		
		long a, b, c;
		double hash;
		a = fileName.hashCode();
		b = (long) r;
		c = uname.hashCode();
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, c >> 13);
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 8));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 13));
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, (c >> 12));
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 16));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 5));
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, (c >> 3));
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 10));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 15));
		int res1 = (int)c % 12345;
		hash = res1 / 12345.0;
		if(hash < 0)
			return -hash;
		else
			return hash;
	}		
}
