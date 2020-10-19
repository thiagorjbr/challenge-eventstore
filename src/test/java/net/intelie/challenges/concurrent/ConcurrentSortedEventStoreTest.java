package net.intelie.challenges.concurrent;

import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import net.intelie.challenges.Event;

public class ConcurrentSortedEventStoreTest {

	@Test
	public void threadTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnable(con));
		Thread t2 = new Thread(new MyRunnable(con));
		Thread t3 = new Thread(new MyRunnable(con));
		Thread t4 = new Thread(new MyRunnable(con));
		Thread t5 = new Thread(new MyRunnable(con));
		Thread t6 = new Thread(new MyRunnable(con));
		Thread t7 = new Thread(new MyRunnable(con));
		Thread t8 = new Thread(new MyRunnable(con));
		Thread t9 = new Thread(new MyRunnable(con));
		Thread t10 = new Thread(new MyRunnable(con));

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

		System.out.println(con.size());
		System.out.println(con.length());
		System.out.println(con.isSorted());
		System.out.println(con.checkPoint.size());
		
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

		while (current.next != null && current != null) {
			if (current.value.timestamp() > current.next.value.timestamp()) {
				return false;
			}
			current = current.next;
		}
		return true;
	}
}

class MyRunnable implements Runnable {
	private ConcurrentSortedEventStore con = null;

	public MyRunnable(ConcurrentSortedEventStore con) {
		this.con = con;
	}

	@Override
	public void run() {
		long init = System.currentTimeMillis();
		for (int i = 0; i < 35000; i++) {
			Event event = new Event("teste", new Random().nextInt(2000000));
			con.insert(event);
		}
		System.out.println(
				"execution time " + Thread.currentThread().getName() + ": " + (System.currentTimeMillis() - init));
	}

}
