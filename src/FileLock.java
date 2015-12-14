import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileLock{
	static class Writer implements Runnable{
		String path;
		String content;

		Writer(String path,String cotent){
			this.content=content;
			this.path=path;
		}

		@Override
		public void run(){
			for(;;){
				File f=new File(path);
				FileOutputStream fos=null;
				try{
					fos=new FileOutputStream(f);
					fos.write(content.getBytes());
				}catch(FileNotFoundException e){
					e.printStackTrace();
				}catch(IOException e){
					e.printStackTrace();
				}finally{
					if(fos!=null){
						try{
							fos.close();
						}catch(IOException e){
						}
					}
				}
				try{
					Thread.sleep(100);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
		}
	}

	static class Reader implements Runnable{
		String path;

		Reader(String path){
			this.path=path;
		}

		@Override
		public void run(){
			for(;;){
				String content=null;
				try{
					content=new Scanner(new File(path)).useDelimiter("\\Z").next();
				}catch(FileNotFoundException e){
					e.printStackTrace();
				}
				System.out.println(content);
				try{
					Thread.sleep(1000);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[]) throws IOException, InterruptedException{
		Path tp=Files.createTempFile(null,null);
		String pathname=tp.toFile().getAbsolutePath();
		System.out.println(pathname);
		ExecutorService p=Executors.newCachedThreadPool();
		for(int i=0;i<3;++i){
			p.submit(new Writer(pathname,"Thread "+i));
		}
		p.submit(new Reader(pathname));
		Scanner reader=new Scanner(System.in);
		reader.next();
	}
}
