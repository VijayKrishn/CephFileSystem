package req;

import java.util.List;

public interface RequestCallback{
	List<Integer> call(Request request);
}
