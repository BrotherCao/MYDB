package tm;

import com.brocao.transactionManager.TransactionManager;
import org.junit.Test;

public class TmTest {

    @Test
    public void tmTest() {
        TransactionManager tm = TransactionManager.create("tmTestDb1");
        tm.begin();
        tm.begin();
        tm.begin();
        tm.begin();
        tm.begin();
        tm.commit(2);
        tm.abort(3);
        System.out.println("事务2是否撤销 ：" + tm.isAborted(2));
        System.out.println("事务3是否撤销 ：" + tm.isAborted(3));
    }

    @Test
    public void tmTestOpen() {
        TransactionManager tm = TransactionManager.open("tmTestDb1");
        System.out.println("事务2是否撤销 ：" + tm.isAborted(2));
        System.out.println("事务3是否撤销 ：" + tm.isAborted(3));
    }
}
