package net.intelie.challenges.concurrent;

import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class ConcurrentSortedEventStoreTest {
	
	@Test
	public void ordinationTest() {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);
		
		{
			Event event = new Event("teste", 2);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 1);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 7);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 5);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 45);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 7);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 3);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 6);
			con.insert(event);
		}
		
		System.out.println(size(con.checkPoint.first()));
		System.out.println(con.length());
		System.out.println(isSorted(con.checkPoint.first()));
		System.out.println(con.checkPoint.size());
	}

	@Test
	public void threadInsertTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con));
		Thread t5 = new Thread(new MyRunnableInsert(con));
		Thread t6 = new Thread(new MyRunnableInsert(con));
		Thread t7 = new Thread(new MyRunnableInsert(con));
		Thread t8 = new Thread(new MyRunnableInsert(con));
		Thread t9 = new Thread(new MyRunnableInsert(con));
		Thread t10 = new Thread(new MyRunnableInsert(con));

		long init = System.currentTimeMillis();

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();
		t8.start();
		t9.start();
		t10.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();
		t8.join();
		t9.join();
		t10.join();

		System.out.println("execution time: " + (System.currentTimeMillis() - init));

		System.out.println(size(con.checkPoint.first()));
		System.out.println(con.length());
		System.out.println(isSorted(con.checkPoint.first()));
		System.out.println(con.checkPoint.size());
		
	}
	
	@Test
	public void threadInsertRemoveTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con));
		Thread t5 = new Thread(new MyRunnableInsert(con));
		Thread t6 = new Thread(new MyRunnableInsert(con));
		Thread t7 = new Thread(new MyRunnableInsert(con));
		Thread t8 = new Thread(new MyRunnableInsert(con));
		Thread t9 = new Thread(new MyRunnableInsert(con));
		Thread t10 = new Thread(new MyRunnableInsert(con));
		Thread t11 = new Thread(new MyRunnableRemove(con, "delete"));

		long init = System.currentTimeMillis();

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();
		t8.start();
		t9.start();
		t10.start();
		t11.start();
		
		Thread.sleep(5000);
		
		con.removeAll("delete");
		
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();
		t8.join();
		t9.join();
		t10.join();
		t11.join();
		
		

		System.out.println("execution time: " + (System.currentTimeMillis() - init));

		System.out.println(size(con.checkPoint.first()));
		System.out.println(con.length());
		System.out.println(isSorted(con.checkPoint.first()));
		System.out.println(con.checkPoint.size());
		
	}
	
	@Test
	public void threadInsertQueryTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t2 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t3 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t4 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t5 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t6 = new Thread(new MyRunnableInsert(con));
		Thread t7 = new Thread(new MyRunnableInsert(con));
		Thread t8 = new Thread(new MyRunnableInsert(con));
		Thread t9 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t10 = new Thread(new MyRunnableInsert(con, "query"));

		long init = System.currentTimeMillis();

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();
		t8.start();
		t9.start();
		t10.start();
		
		Thread.sleep(5000);
		EventIterator ei = con.query("query", 1000, 5000);
		
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();
		t8.join();
		t9.join();
		t10.join();
		

		System.out.println("execution time: " + (System.currentTimeMillis() - init));

		System.out.println(size(con.checkPoint.first()));
		System.out.println(con.length());
		System.out.println(isSorted(con.checkPoint.first()));
		System.out.println(con.checkPoint.size());
		System.out.println("newIterator: " + size((ConcurrentEventIterator) ei));
		System.out.println("newIterator: " + isSorted((ConcurrentEventIterator) ei));
	}
	
	@Test
	public void threadInsertQueryRemoveTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con));
		Thread t5 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t6 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t7 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t8 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t9 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t10 = new Thread(new MyRunnableInsert(con, "query"));
		Thread t11 = new Thread(new MyRunnableRemove(con, "query"));

		long init = System.currentTimeMillis();

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();
		t8.start();
		t9.start();
		t10.start();
		
		Thread.sleep(3000);
		EventIterator ei = con.query("query", 10000, 15000);
		
		Thread.sleep(2000);
		t11.start();
		
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();
		t8.join();
		t9.join();
		t10.join();
		t11.join();
		

		System.out.println("execution time: " + (System.currentTimeMillis() - init));

		System.out.println(size(con.checkPoint.first()));
		System.out.println(con.length());
		System.out.println(isSorted(con.checkPoint.first()));
		System.out.println(con.checkPoint.size());
		System.out.println("newIterator: " + size((ConcurrentEventIterator) ei));
		System.out.println("newIterator: " + isSorted((ConcurrentEventIterator) ei));
		System.out.println(size((ConcurrentEventIterator) con.query("query", 10000, 15000)));
	}

	private int size(ConcurrentEventIterator current) {
		int count = 0;

		while (current != null) {
			ReentrantLock lock = current.lock;
			lock.lock();
			try {
				++count;
				current = current.next;
			} finally {
				lock.unlock();
			}
		}

		return count;
	}

	private boolean isSorted(ConcurrentEventIterator current) {

		while (current != null && current.next != null) {
			if (current.value.timestamp() > current.next.value.timestamp()) {
				return false;
			}
			current = current.next;
		}
		return true;
	}
}

class MyRunnableInsert implements Runnable {
	private ConcurrentSortedEventStore con = null;
	private String type = "teste";
	
	public MyRunnableInsert(ConcurrentSortedEventStore con) {
		this.con = con;
	}

	public MyRunnableInsert(ConcurrentSortedEventStore con, String type) {
		this.con = con;
		this.type = type;
	}

	@Override
	public void run() {
		long init = System.currentTimeMillis();
		
		for (int i = 0; i < 35000; i++) {
			Event event = new Event(type, new Random().nextInt(2000000));
			con.insert(event);
		}

		System.out.println(
				"execution time " + Thread.currentThread().getName() + ": " + (System.currentTimeMillis() - init));
	}

}

class MyRunnableRemove implements Runnable {
	private ConcurrentSortedEventStore con = null;
	private String type = "teste";
	
	public MyRunnableRemove(ConcurrentSortedEventStore con) {
		this.con = con;
	}

	public MyRunnableRemove(ConcurrentSortedEventStore con, String type) {
		this.con = con;
		this.type = type;
	}

	@Override
	public void run() {
		long init = System.currentTimeMillis();
		
		for (int i = 0; i < 35000; i++) {
			Event event = new Event(type, new Random().nextInt(2000000));
			con.insert(event);
		}

		System.out.println(
				"execution time " + Thread.currentThread().getName() + ": " + (System.currentTimeMillis() - init));
	}

}
