package util;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import req.Rand.ContentSink;
import req.Rand.ContentSrc;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Upload, download, pipeline file
 * From here you know how to DMA copy file directly to socket without extra memory copying.
 */
public class FileHelper{
	private static final Log log=Log.get();
	private static final int bufferSize=1024*256;
	private static final int queueLength=8;

	/**
	 * Upload local disk file to socket channel.
	 *
	 * @param src  local disk file channel
	 * @param dest target socket channel
	 * @param size how many bytes to upload
	 * @throws IOException
	 */
	public static void upload(FileChannel src,SocketChannel dest,long size) throws IOException{
		upload(src,dest,size,0);
	}

	/**
	 * Upload local disk file to socket channel, this method will upload up to size-position bytes.
	 *
	 * @param src      local disk file channel
	 * @param dest     target socket channel
	 * @param size     the last position of file channel to be upload
	 * @param position the start position of file channel to be upload
	 * @throws IOException
	 */
	public static void upload(FileChannel src,SocketChannel dest,long size,long position) throws IOException{
		while(position<size)
			position+=src.transferTo(position,size-position,dest);
	}

	public static void upload(ContentSrc src,SocketChannel dest,long size) throws IOException{
		long position=0;
		while(position<size)
			position+=src.transferTo(dest,size-position);
	}

	/**
	 * Download bytes from socket channel to local disk file channel, this method will download up to size-position bytes.
	 *
	 * @param src      socket channel to be read
	 * @param dest     file channel to be write
	 * @param size     the last position of file channel to be write
	 * @param position the start position if file channel to be write
	 * @throws IOException
	 */
	public static void download(SocketChannel src,FileChannel dest,long size,long position) throws IOException{
		while(position<size)
			position+=dest.transferFrom(src,position,size-position);
	}

	/**
	 * Download bytes from socket channel to local disk file channel, this method will download up to size-position bytes.
	 *
	 * @param src  socket channel to be read
	 * @param dest file channel to be write
	 * @param size the total bytes of file channel to be write
	 * @throws IOException
	 */
	public static void download(SocketChannel src,FileChannel dest,long size) throws IOException{
		download(src,dest,size,0);
	}

	public static void download(SocketChannel src,ContentSink dest,long size) throws IOException{
		long position=0;
		while(position<size)
			position+=dest.transferFrom(src,size-position);
	}

	/**
	 * Direct a socket byte stream to another socket channel and also local disk file channel.
	 *
	 * @param executor a thread pool, can be the one returned by Session.getExecutor()
	 * @param src      src socket channel
	 * @param fDest    target local disk file channel
	 * @param dest     target socket channel
	 * @param size     last position of file channel to be write
	 * @param position start position of file channel to be write
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void pipe(ExecutorService executor,
	                        SocketChannel src,FileChannel fDest,SocketChannel dest,long size,long position)
			throws ExecutionException, InterruptedException, IOException{
		pipe(executor,src,fDest,dest,size,position,0,0);
	}

	/**
	 * Direct a socket byte stream to another socket channel and also local disk file channel.
	 *
	 * @param executor a thread pool, can be the one returned by Session.getExecutor()
	 * @param src      src socket channel
	 * @param fDest    target local disk file channel
	 * @param dest     target socket channel
	 * @param size     last position of file channel to be write
	 * @param position start position of file channel to be write
	 * @param timeout  if timeout>0, operation aborts when timeout in ms is hit.
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void pipe(ExecutorService executor,
	                        SocketChannel src,FileChannel fDest,SocketChannel dest,long size,long position,
	                        long start,long timeout)
			throws ExecutionException, InterruptedException, IOException{
		CyclicBarrier barrier=new CyclicBarrier(2);
		GenericObjectPool<ByteBuffer> bufferRing=new GenericObjectPool<>(new ByteBufferFactory());
		BlockingQueue<ByteBuffer> socketQueue=new ArrayBlockingQueue<>(queueLength);
		BlockingQueue<ByteBuffer> fileQueue=new ArrayBlockingQueue<>(queueLength);
		Reader reader=new Reader(src,size,position,bufferRing,socketQueue,fileQueue);
		FileWriter fileWriter=new FileWriter(fDest,barrier,size,position,fileQueue);
		SocketWriter socketWriter=new SocketWriter(dest,barrier,size,position,socketQueue,bufferRing);
		List<Future> futures=new ArrayList<>();
		futures.add(executor.submit(reader));
		futures.add(executor.submit(socketWriter));
		futures.add(executor.submit(fileWriter));
		if(timeout<=0)
			for(int i=0;i<3;++i){
				Future future=futures.get(i);
				try{
					future.get();
				}catch(InterruptedException|ExecutionException e){
					log.w(e);
					for(int j=i+1;j<3;++j)
						futures.get(j).cancel(true);
					throw e;
				}
			}
		else{
			int i=0;
			while(start+timeout>=System.currentTimeMillis()){
				try{
					futures.get(i).get();
				}catch(InterruptedException|ExecutionException e){
					log.w(e);
					for(int j=i+1;j<3;++j)
						futures.get(j).cancel(true);
					throw e;
				}
				if(++i==3) break;
			}
		}
	}

	/**
	 * Direct a socket byte stream to another socket channel and also local disk file channel.
	 *
	 * @param executor a thread pool, can be the one returned by Session.getExecutor()
	 * @param src      src socket channel
	 * @param fDest    target local disk file channel
	 * @param dest     target socket channel
	 * @param size     last position of file channel to be write
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void pipe(ExecutorService executor,
	                        SocketChannel src,FileChannel fDest,SocketChannel dest,long size)
			throws ExecutionException, InterruptedException, IOException{
		pipe(executor,src,fDest,dest,size,0);
	}

	protected static class Reader implements Callable{
		private SocketChannel src;
		private long position;
		private long size;
		private GenericObjectPool<ByteBuffer> bufferRing;
		private BlockingQueue<ByteBuffer> socketQueue;
		private BlockingQueue<ByteBuffer> fileQueue;

		Reader(SocketChannel src,long size,long position,GenericObjectPool<ByteBuffer> bufferRing,
		       BlockingQueue<ByteBuffer> socketQueue,
		       BlockingQueue<ByteBuffer> fileQueue){
			this.src=src;
			this.size=size;
			this.position=position;
			this.bufferRing=bufferRing;
			this.socketQueue=socketQueue;
			this.fileQueue=fileQueue;
		}

		@Override
		public Object call() throws Exception{
			while(position<size){
				ByteBuffer buffer=bufferRing.borrowObject();
				while(buffer.hasRemaining() && position<size){
					long read_once=src.read(buffer);
					if(read_once<0) throw new EOFException();
					position+=read_once;
				}
				buffer.flip();
				ByteBuffer fileBuffer=buffer.duplicate();
				socketQueue.put(buffer);
				fileQueue.put(fileBuffer);
			}
			return null;
		}
	}

	protected static class FileWriter implements Callable{
		private CyclicBarrier barrier;
		private long size;
		private long position;
		private FileChannel fDest;
		private BlockingQueue<ByteBuffer> fileQueue;

		FileWriter(FileChannel fDest,CyclicBarrier barrier,long size,long position,
		           BlockingQueue<ByteBuffer> fileQueue){
			this.barrier=barrier;
			this.size=size;
			this.position=position;
			this.fDest=fDest;
			this.fileQueue=fileQueue;
		}

		@Override
		public Object call() throws Exception{
			fDest.position(position);
			while(position<size){
				ByteBuffer buffer=fileQueue.take();
				while(buffer.hasRemaining() && position<size)
					position+=fDest.write(buffer);
				if(position<size)
					barrier.await();
			}
			return null;
		}
	}

	protected static class SocketWriter implements Callable{
		GenericObjectPool<ByteBuffer> bufferRing;
		private CyclicBarrier barrier;
		private long size;
		private long position;
		private SocketChannel dest;
		private BlockingQueue<ByteBuffer> socketQueue;

		SocketWriter(SocketChannel dest,CyclicBarrier barrier,long size,long position,
		             BlockingQueue<ByteBuffer> socketQueue,
		             GenericObjectPool<ByteBuffer> bufferRing){
			this.barrier=barrier;
			this.size=size;
			this.position=position;
			this.dest=dest;
			this.socketQueue=socketQueue;
			this.bufferRing=bufferRing;
		}

		@Override
		public Object call() throws Exception{
			while(position<size){
				ByteBuffer buffer=socketQueue.take();
				while(buffer.hasRemaining() && position<size)
					position+=dest.write(buffer);
				if(position<size)
					barrier.await();
				bufferRing.returnObject(buffer);
			}
			return null;
		}
	}

	static class ByteBufferFactory extends BasePooledObjectFactory<ByteBuffer>{
		@Override
		public void passivateObject(PooledObject<ByteBuffer> p) throws Exception{
			super.passivateObject(p);
			p.getObject().reset();
		}

		@Override
		public ByteBuffer create() throws Exception{
			return ByteBuffer.allocateDirect(bufferSize);
		}

		@Override
		public PooledObject<ByteBuffer> wrap(ByteBuffer obj){
			return new DefaultPooledObject<>(obj);
		}
	}
}
