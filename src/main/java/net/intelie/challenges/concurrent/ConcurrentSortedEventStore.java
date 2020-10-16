package net.intelie.challenges.concurrent;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

public class ConcurrentSortedEventStore implements EventStore {
	private final int pagination = 5000;
	private ConcurrentEventIterator head;
	private ConcurrentEventIterator mid;
	private AtomicLong midValue;
	
	private AtomicInteger length;

	public ConcurrentSortedEventStore() {
		init();
	}

	@Override
	public void insert(Event event) {
		if (event == null || event.type() == null || event.type().trim().isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		ConcurrentEventIterator current;

		current = head;
		if (midValue.get() >= 0 && midValue.get() < event.timestamp()) {
			current = mid;
		} else {
			current = head;
		}

		length.incrementAndGet();

		current.lock.lock();

		int lockCount = current.lock.getHoldCount();

		if (lockCount > 1) {
			System.out.println("hold: " + lockCount);
		}

		if (current.value == null) {
			current.value = event;
			current.lock.unlock();
			return;
		}

		if (current.next == null) {
			try {
				if (current.value.timestamp() > event.timestamp()) {
					ConcurrentEventIterator aux = new ConcurrentEventIterator(current.next, current.value);
					current.value = event;
					current.next = aux;
				} else {
					current.next = new ConcurrentEventIterator(current.next, event);
				}
				mid = current;
				return;
			} finally {
				current.lock.unlock();
			}
		}

		ConcurrentEventIterator prev = null;

		try {
			long count = 0L;
			do {
				if (current.value.timestamp() > event.timestamp()) {
					ConcurrentEventIterator aux = new ConcurrentEventIterator(current.next, current.value);
					current.value = event;
					current.next = aux;
					current.lock.unlock();
					return;
				}

				if (prev != null) {
					prev.lock.unlock();
				}

				if (++count == Math.round(length.get() / 2)) {
					mid = current;
					midValue.set(event.timestamp());
				}

				prev = current;
				current = current.next;

				if (current != null) {
					current.lock.lock();
				}
			} while (current != null);

			prev.next = new ConcurrentEventIterator(event);
		} finally {
			if (prev != null) {
				prev.lock.unlock();
			}
		}
	}

	@Override
	public void removeAll(String type) {
		init();
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {

		synchronized (head) {

		}

		return null;
	}

	public int length() {
		return length.get();
	}

	private void init() {
		head = new ConcurrentEventIterator();
		length = new AtomicInteger(0);
		midValue = new AtomicLong(-1L);
	}

	public static void main(String[] args) throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore();

		Thread t1 = new Thread(new MyRunnable(con));
		Thread t2 = new Thread(new MyRunnable(con));
		Thread t3 = new Thread(new MyRunnable(con));
		Thread t4 = new Thread(new MyRunnable(con));
		Thread t5 = new Thread(new MyRunnable(con));
		Thread t6 = new Thread(new MyRunnable(con));
		Thread t7 = new Thread(new MyRunnable(con));

		long init = System.currentTimeMillis();

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();

		System.out.println("execution time: " + (System.currentTimeMillis() - init));

		long init2 = System.currentTimeMillis();
		Event event = new Event("teste", 200001);
		con.insert(event);
		System.out.println("execution time2: " + (System.currentTimeMillis() - init2));

		System.out.println(con.size());
		System.out.println(con.length());
		System.out.println(con.isSorted());

//		Thread.sleep(30000);
	}

	public int size() {
		ConcurrentEventIterator current = head;
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

	public boolean isSorted() {
		ConcurrentEventIterator current = head;

		while (current.next != null && current != null) {
			if (current.value.timestamp() > current.next.value.timestamp()) {
				return false;
			}
			current = current.next;
		}
		return true;
	}

	@Override
	public String toString() {
		return "ConcurrentSortedEventStore [head=" + head + "]";
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
		for (int i = 0; i < 20000; i++) {
			Event event = new Event("teste", new Random().nextInt(200000));
			con.insert(event);
		}
		System.out.println(
				"execution time " + Thread.currentThread().getName() + ": " + (System.currentTimeMillis() - init));
	}

}
