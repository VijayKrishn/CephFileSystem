package sample.Request;

import net.IOControl;

import org.ini4j.Wini;

import req.DynamicTree;
import req.Rand.ExpGenerator;
import req.Rand.RequestGenerator;
import req.Rand.UniformGenerator;
import req.Rand.ZipfGenerator;
import req.Request;
import req.RequestCallback;
import req.StaticTree;
import sample.ClientOps;
import util.AutoLock;
import util.Log;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReqGenerator{
	static Log log=Log.get();
	public static IOControl control = new IOControl();
	public static class RequestThread implements Runnable{
		UniformGenerator uniform;
		ExpGenerator exp;
		ZipfGenerator zipf;
		RequestGenerator reqGen;
		Map<Request.ReqType, RequestCallback> callbacks;
		int unit;
		int max_rand;
		int max_seq;
		StaticTree sTree;
		DynamicTree dTree;
		CountDownLatch start;
		ReadWriteLock lock;
		double dynamicInsertRatio;

		public RequestThread(UniformGenerator uniform,
		                     ExpGenerator exp,
		                     ZipfGenerator zipf,
		                     RequestGenerator reqGen,
		                     Map<Request.ReqType, RequestCallback> callbacks,
		                     int unit,
		                     int max_rand,
		                     int max_seq,
		                     StaticTree sTree,
		                     DynamicTree dTree,
		                     double dynamicInsertRatio,
		                     ReadWriteLock lock,
		                     CountDownLatch start){
			this.uniform=uniform;
			this.exp=exp;
			this.zipf=zipf;
			this.reqGen=reqGen;
			this.callbacks=callbacks;
			this.unit=unit;
			this.max_rand=max_rand;
			this.max_seq=max_seq;
			this.sTree=sTree;
			this.dTree=dTree;
			this.start=start;
			this.lock=lock;
			this.dynamicInsertRatio=dynamicInsertRatio;
		}

		boolean RWSize(Request request){
			if(request.end<=0) return false;
			if((request.end-request.start)>unit){
				boolean seq=(request.type==Request.ReqType.SEQ_READ || request.type==Request.ReqType.SEQ_WRITE);
				long len=(uniform.nextLong(Math.min(request.end/unit,seq ? max_seq : max_rand)+1))*unit;
//				System.out.println("len:"+len+" end:"+request.end+" unit:"+unit+" r:"+(int)(request.end/unit)+" minus:"+(request.end-len));
				request.start=uniform.nextLong(request.end-len+1);
				request.end=request.start+len;
			}
			return true;
		}

		@Override
		public void run(){
			int sTreeFiles=sTree.getFileSize();
			int sTreeHeadFiles=(int)(sTreeFiles*dynamicInsertRatio);
			int sTreeDirs=sTree.getNonEmptyDirSize();
			int sTreeHeadDirs=(int)(sTreeDirs*dynamicInsertRatio);
			long reqCounter=0;
			long reqTimeSum=0;
			long overheadSum=0;
			long intervalSum=0;
			while(!Thread.currentThread().isInterrupted()){
				long t1=System.currentTimeMillis();
				long interval=exp.nextInt();
				long stopExpectation=t1+interval;
				Request.ReqType type=reqGen.next();
				Request request=null;
				if(dTree==null){
					if(type==Request.ReqType.LS)
						request=sTree.ls(zipf.nextInt(sTreeDirs-1));
					else if(type==Request.ReqType.APPEND){
						request=sTree.append(zipf.nextInt(sTreeFiles-1),(uniform.nextLong(max_seq)+1)*unit);
					}else if(type==Request.ReqType.SEQ_READ ||
							type==Request.ReqType.SEQ_WRITE ||
							type==Request.ReqType.RANDOM_READ ||
							type==Request.ReqType.RANDOM_WRITE){
						request=sTree.fileInfo(zipf.nextInt(sTreeFiles-1));
						request.type=type;
						if(!RWSize(request)) continue;
					}else continue;
					long t2=System.currentTimeMillis();
					callbacks.get(type).call(request);
					long t4=System.currentTimeMillis();
					reqCounter+=1;
					reqTimeSum+=(t4-t2);
					overheadSum+=(t2-t1);
				}else{
					try(AutoLock auto=AutoLock.lock(lock.readLock())){
						if(type==Request.ReqType.CREATE_DIR ||
								type==Request.ReqType.CREATE_FILE ||
								type==Request.ReqType.DELETE ||
								type==Request.ReqType.RMDIR){
							lock.readLock().unlock();
							lock.writeLock().lock();
							if(type==Request.ReqType.CREATE_DIR){
								request=dTree.createDir(zipf.nextInt(dTree.getAllDirSize()-1));
							}else if(type==Request.ReqType.CREATE_FILE){
								request=dTree.createFile(zipf.nextInt(dTree.getAllDirSize()-1));
							}else if(type==Request.ReqType.DELETE){
								if(dTree.getFileSize()==0){
									lock.writeLock().unlock();
									continue;
								}
								request=dTree.delete(zipf.nextInt(dTree.getFileSize()-1));
							}else if(type==Request.ReqType.RMDIR){
								if(dTree.getEmptyDirSize()==0){
									lock.writeLock().unlock();
									continue;
								}
								request=dTree.rmdir(zipf.nextInt(dTree.getEmptyDirSize()-1));
							}
							lock.readLock().lock();
							lock.writeLock().unlock();
						}else{
							if(type==Request.ReqType.LS){
								int n=zipf.nextInt(sTreeDirs+dTree.getNonEmptyDirSize()-1);
								if(n<sTreeHeadDirs)
									request=sTree.ls(n);
								else{
									n-=sTreeHeadDirs;
									if(n<dTree.getNonEmptyDirSize())
										request=dTree.ls(n);
									else{
										n=n-dTree.getNonEmptyDirSize()+sTreeHeadDirs;
										request=sTree.ls(n);
									}
								}
							}else{
								int n=zipf.nextInt(sTreeFiles+dTree.getFileSize()-1);
								StaticTree t=sTree;
								if(n>=sTreeHeadFiles){
									n-=sTreeHeadFiles;
									if(n<dTree.getFileSize()) t=dTree;
									else n=n-dTree.getFileSize()+sTreeHeadFiles;
								}
								if(type==Request.ReqType.APPEND){
									request=t.append(n,(uniform.nextLong(max_seq)+1)*unit);
								}else if(type==Request.ReqType.SEQ_READ ||
										type==Request.ReqType.SEQ_WRITE ||
										type==Request.ReqType.RANDOM_READ ||
										type==Request.ReqType.RANDOM_WRITE){
									request=t.fileInfo(n);
									request.type=type;
									if(!RWSize(request)) continue;
								}else continue;
							}
						}
						int loop=0;
						overheadSum+=(System.currentTimeMillis()-t1);
						do{
							long t2=System.currentTimeMillis();
							callbacks.get(type).call(request);
							reqTimeSum+=(System.currentTimeMillis()-t2);
							loop+=1;
							request=request.next;
						}while(request!=null);
						reqCounter+=loop;
						while((--loop)>0){
							stopExpectation+=exp.nextInt();
						}
					}
				}
				long t3=System.currentTimeMillis();
				if(t3<stopExpectation) try{
					Thread.sleep(stopExpectation-t3);
				}catch(InterruptedException ignored){
					break;
				}
				intervalSum+=(System.currentTimeMillis()-t1);
			}
			log.i(String.format("Total req: %d, average request time: %d ms, overhead: %d ms, inteval: %d ms",
					reqCounter,reqTimeSum/reqCounter,overheadSum/reqCounter,intervalSum/reqCounter));
			start.countDown();
		}
	}

	public static ExecutorService generate(
			//  input files
			String staticFile,  //  immutable tree file
			String staticRankFile,    //  ranking file, nullable
			String dynamicFile,    //  mutable tree file, nullable
			double dynamicInsertRatio,  //  [0,1], specify which point should dynamic tree be inserted into static tree.
			//  parameters
			//  exponential generator
			double lambda,  //  curve
			int duration,   //  tail
			//  zipf generator
			double alpha,
			//  io
			int unit,
			int max_rand,
			int max_seq,
			//  request
			Map<Request.ReqType, Double> ratio,
			Map<Request.ReqType, RequestCallback> callbacks,
			CountDownLatch start,
			int numThreads
	) throws IOException{
		if(ratio.size()!=callbacks.size())
			throw new IllegalArgumentException(String.format("Length of request ratio %d and callback %d do not match.",ratio.size(),callbacks.size()));
		if(dynamicFile!=null && (dynamicInsertRatio<0 || dynamicInsertRatio>1))
			throw new IllegalArgumentException("dynamicInsertRatio out of range: "+dynamicInsertRatio);
		UniformGenerator uniform=new UniformGenerator();
		StaticTree sTree=StaticTree.getStaticTree(staticFile,uniform);
		if(staticRankFile!=null) sTree.shuffleFilesUneven(staticRankFile);
		DynamicTree dTree=dynamicFile!=null ? DynamicTree.getDynamicTree(dynamicFile,uniform) : null;
		ReadWriteLock lock=dynamicFile!=null ? new ReentrantReadWriteLock() : null;
		ExpGenerator exp=new ExpGenerator(lambda,duration,uniform);
		ZipfGenerator zipf=new ZipfGenerator(alpha,sTree.getFileSize(),uniform);
		RequestGenerator reqGen=new RequestGenerator(ratio,uniform);
		ExecutorService pool=Executors.newFixedThreadPool(numThreads);
		for(int i=0;i<numThreads;++i){
			pool.execute(new RequestThread(uniform,exp,zipf,reqGen,callbacks,unit,max_rand,max_seq,sTree,dTree,dynamicInsertRatio,lock,start));
		}
		return pool;
	}

	static class dump implements RequestCallback{

		@Override
		public List<Integer> call(Request request){
			//System.out.println(request.toString());
			//System.out.println(request.end);
			if(request.type == Request.ReqType.SEQ_READ)
				return ClientOps.readRequest(request.path.trim(), control);
			else if(request.type == Request.ReqType.SEQ_WRITE)
				return ClientOps.writeRequest(request.path.trim(), control);
//			log.i(request.toString());
			return null;
		}
	}

	public static void main(String args[]) throws IOException, InterruptedException{
		Wini conf=new Wini(new File("files/request.ini"));
		Map<Request.ReqType, Double> ratio=new HashMap<>();
		Map<Request.ReqType, RequestCallback> callbacks=new HashMap<>();
		RequestCallback call=new dump();
		Map<String, String> map=conf.get("request");
		String seq_read=map.get("seq_read");
		if(seq_read!=null){
			ratio.put(Request.ReqType.SEQ_READ,Double.parseDouble(seq_read));
			callbacks.put(Request.ReqType.SEQ_READ,call);
		}
		String rand_read=map.get("rand_read");
		if(rand_read!=null){
			ratio.put(Request.ReqType.RANDOM_READ,Double.parseDouble(rand_read));
			callbacks.put(Request.ReqType.RANDOM_READ,call);
		}
		String seq_write=map.get("seq_write");
		if(seq_write!=null){
			ratio.put(Request.ReqType.SEQ_WRITE,Double.parseDouble(seq_write));
			callbacks.put(Request.ReqType.SEQ_WRITE,call);
		}
		String rand_write=map.get("rand_write");
		if(rand_write!=null){
			ratio.put(Request.ReqType.RANDOM_WRITE,Double.parseDouble(rand_write));
			callbacks.put(Request.ReqType.RANDOM_WRITE,call);
		}
		String append=map.get("append");
		if(append!=null){
			ratio.put(Request.ReqType.APPEND,Double.parseDouble(append));
			callbacks.put(Request.ReqType.APPEND,call);
		}
		String create_dir=map.get("create_dir");
		if(create_dir!=null){
			ratio.put(Request.ReqType.CREATE_DIR,Double.parseDouble(create_dir));
			callbacks.put(Request.ReqType.CREATE_DIR,call);
		}
		String create_file=map.get("create_file");
		if(create_file!=null){
			ratio.put(Request.ReqType.CREATE_FILE,Double.parseDouble(create_file));
			callbacks.put(Request.ReqType.CREATE_FILE,call);
		}
		String delete=map.get("delete");
		if(delete!=null){
			ratio.put(Request.ReqType.DELETE,Double.parseDouble(delete));
			callbacks.put(Request.ReqType.DELETE,call);
		}
		String ls=map.get("ls");
		if(ls!=null){
			ratio.put(Request.ReqType.LS,Double.parseDouble(ls));
			callbacks.put(Request.ReqType.LS,call);
		}
		int threads=8;
		CountDownLatch start=new CountDownLatch(threads);
		ExecutorService service=generate("files/test.txt",null,"files/test2.txt",
				conf.get("request","dyn_ratio",Double.class),
				conf.get("time","lambda",Double.class),
				conf.get("time","duration",Integer.class),
				conf.get("zipf","alpha",Double.class),
				conf.get("io","unit",Integer.class),
				conf.get("io","rand",Integer.class),
				conf.get("io","seq",Integer.class),
				ratio,callbacks,start,threads);
		try{
			System.out.println("countdown");
			start.await(100,TimeUnit.SECONDS);
		}catch(InterruptedException ignored){
		}finally{
			service.shutdownNow();
			service.awaitTermination(1,TimeUnit.SECONDS);
		}
	}
}
