package spendreport;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

/**
 * Created by HE31 on 2020 /12/6 17:12
 */
public class CustomSource implements SourceFunction<Integer> {
    private  boolean isRun = true;
    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (isRun){
            sourceContext.collect(new Random().nextInt(10));
            Thread.sleep(100);
        }
    }

    // 取消方法
    @Override
    public void cancel() {
        isRun = false;
    }
}
