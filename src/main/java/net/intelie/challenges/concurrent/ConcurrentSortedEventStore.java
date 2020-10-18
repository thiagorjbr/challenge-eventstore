package net.intelie.challenges.concurrent;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

public class ConcurrentSortedEventStore implements EventStore {
	private static final int DEFAULT_PAGINATION_CHECKPOINT = 10000;

	private final int paginationCheckPoint;
	private final int maxCheckPoints;
	
	ConcurrentSkipListSet<ConcurrentEventIterator> checkPoint;
	
	private AtomicInteger length;

	public ConcurrentSortedEventStore() {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = this.DEFAULT_PAGINATION_CHECKPOINT;
		this.maxCheckPoints = 0;
	}
	
	public ConcurrentSortedEventStore(int paginationCheckPoint, int maxCheckPoints) {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = paginationCheckPoint;
		this.maxCheckPoints = 0;
	}
	
	public ConcurrentSortedEventStore(int maxCheckPoints) {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = this.DEFAULT_PAGINATION_CHECKPOINT;
		this.maxCheckPoints = maxCheckPoints;
	}

	@Override
	public void insert(Event event) {
		if (event == null || event.type() == null || event.type().trim().isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		ConcurrentEventIterator current = checkPoint.first();
		int posCheckPoint = 0;
		ConcurrentEventIterator virtualCurrent = checkPoint.first();
		
		{
			Iterator<ConcurrentEventIterator> it = checkPoint.descendingIterator();
			int i = checkPoint.size();
			while (it.hasNext()) {
				ConcurrentEventIterator c = it.next();
				if (c.value != null && c.value.timestamp() < event.timestamp()) {
					current = c;
					virtualCurrent = c;
					posCheckPoint = --i;
					break;
				}
			}
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
				
				count++;
				
				long countPos = paginationCheckPoint * posCheckPoint + count;
				if (countPos % paginationCheckPoint == 0) {
					int pos = (int) Math.floorDiv(countPos, paginationCheckPoint);
					if (checkPoint.size()-1 < pos && maxCheckPoints <= 0 ? checkPoint.size() <= Math.floorDiv(length.get(), paginationCheckPoint) : checkPoint.size() <= maxCheckPoints) {
						checkPoint.add(current);
//						System.out.println("paginationCheckPoint: " + countPos + "[" + paginationCheckPoint + ", " + posCheckPoint + ", " + count + "]" + " position: " + pos + " current: " + current.value.timestamp());
					} else {
						checkPoint.subSet(virtualCurrent, current);
					}
				}
				
				if (prev != null) {
					prev.lock.unlock();
				}

				prev = current;
				if (current.next != null) {
					current.next.lock.lock();
				}
				current = current.next;

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
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {

		return null;
	}

	public int length() {
		return length.get();
	}

	public static void main(String[] args) throws InterruptedException {
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

//		long init2 = System.currentTimeMillis();
//		Event event = new Event("teste", 200001);
//		con.insert(event);
//		System.out.println("execution time2: " + (System.currentTimeMillis() - init2));

		System.out.println(con.size());
		System.out.println(con.length());
		System.out.println(con.isSorted());
		System.out.println(con.checkPoint.size());
		
//		for (ConcurrentEventIterator item : con.checkPoint) {
//			System.out.println(item.value.timestamp());
//		}

//		Thread.sleep(30000);
	}

	public int size() {
		ConcurrentEventIterator current = checkPoint.first();
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
		ConcurrentEventIterator current = checkPoint.first();

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

class EventComparator implements Comparator<ConcurrentEventIterator> {

	@Override
	public int compare(ConcurrentEventIterator o1, ConcurrentEventIterator o2) {
		return (int) (o1.value == null || o2.value == null ? 0 : (o1.value.timestamp() - o2.value.timestamp()));
	}
	
}
