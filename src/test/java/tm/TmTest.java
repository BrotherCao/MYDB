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
        System.out.println("����2�Ƿ��� ��" + tm.isAborted(2));
        System.out.println("����3�Ƿ��� ��" + tm.isAborted(3));
    }

    @Test
    public void tmTestOpen() {
        TransactionManager tm = TransactionManager.open("tmTestDb1");
        System.out.println("����2�Ƿ��� ��" + tm.isAborted(2));
        System.out.println("����3�Ƿ��� ��" + tm.isAborted(3));
    }
}
