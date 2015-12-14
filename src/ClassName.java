public class ClassName{
	static class Inner{
		public void stack(){
			StackTraceElement[] stElements=Thread.currentThread().getStackTrace();
			for(int i=1;i<stElements.length;i++){
				StackTraceElement ste=stElements[i];
				if(ste.getClassName().indexOf("java.lang.Thread")!=0){
					String[] result=new String[2];
					System.out.println(ste.getClassName());
				}
			}
		}
	}

	public static void main(String args[]){
		new Inner().stack();
	}
}
