package net.intelie.challenges.concurrent;

import java.util.Comparator;
import java.util.Iterator;
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
					if (checkPoint.size() - 1 < pos && maxCheckPoints <= 0
							? checkPoint.size() <= Math.floorDiv(length.get(), paginationCheckPoint)
							: checkPoint.size() <= maxCheckPoints) {
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
		ConcurrentEventIterator current = checkPoint.first();

		while (current != null) {
			ReentrantLock lock = current.lock;
			lock.lock();
			try {
				current = current.next;
			} finally {
				lock.unlock();
			}
		}
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {

		return null;
	}

	public int length() {
		return length.get();
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

	private class EventComparator implements Comparator<ConcurrentEventIterator> {

		@Override
		public int compare(ConcurrentEventIterator o1, ConcurrentEventIterator o2) {
			return (int) (o1.value == null || o2.value == null ? 0 : (o1.value.timestamp() - o2.value.timestamp()));
		}

	}
}
