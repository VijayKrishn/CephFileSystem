import req.Rand.RequestGenerator;
import req.Rand.UniformGenerator;
import req.Request;

import java.util.HashMap;
import java.util.Map;

public class ReqGenTest{
	public static void main(String args[]){
		RandomTest.Frequency<Request.ReqType> freq=new RandomTest.Frequency<>();
		Map<Request.ReqType, Double> ratio=new HashMap<>();
		ratio.put(Request.ReqType.SEQ_READ,10.0);
		ratio.put(Request.ReqType.SEQ_WRITE,5.0);
		ratio.put(Request.ReqType.CREATE_DIR,0.1);
		RequestGenerator gen=new RequestGenerator(ratio,new UniformGenerator());
		for(int i=0;i<2048;++i){
			freq.add(gen.next());
		}
		freq.print();
	}
}
