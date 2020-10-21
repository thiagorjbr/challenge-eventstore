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

	protected ConcurrentSkipListSet<ConcurrentEventIterator> checkPoint;

	private AtomicInteger length;

	public ConcurrentSortedEventStore() {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = DEFAULT_PAGINATION_CHECKPOINT;
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
		this.paginationCheckPoint = DEFAULT_PAGINATION_CHECKPOINT;
		this.maxCheckPoints = maxCheckPoints;
	}

	@Override
	public void insert(Event event) {
		if (event == null) {
			throw new IllegalArgumentException("Event cannot be null");
		} else if (event.type() == null || event.type().trim().isEmpty()) {
			throw new IllegalArgumentException("Type cannot be null or empty");
		}

		ConcurrentEventIterator current = checkPoint.first();
		int posCheckPoint = 0;
		ConcurrentEventIterator virtualCurrent = checkPoint.first();

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

		length.incrementAndGet();

		current.lock.lock();

		if (!current.isValid) {
			current.lock.unlock();
			insert(event);
			return;
		}

		int lockCount = current.lock.getHoldCount();

		if (lockCount > 1) {
			System.out.println("hold: " + lockCount);
		}

		if (current.value == null) {
			current.value = event;
			current.lock.unlock();
			return;
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
		if (type == null || type.trim().isEmpty()) {
			throw new IllegalArgumentException("Type cannot be null or empty");
		}
		
		ConcurrentEventIterator current = checkPoint.first();
		ConcurrentEventIterator prev = null;
		ConcurrentEventIterator next = null;

		try {
			current.lock.lock();
			while (current != null) {
				if (current.next != null) {
					current.next.lock.lock();
					next = current.next;
				} else if (current.value == null) {
					current.lock.unlock();
					return;
				}
				
				boolean permissionToUnlockPrev = true;

				if (current.value.type().equals(type)) {
					length.decrementAndGet();
					if (prev == null) {
						if (next == null) {
							current.value = null;
							current.lock.unlock();
							return;
						} else {
							subSetIf(current, next);
							current.next = null;
							current.isValid = false;
							current.lock.unlock();
							current = next;
							continue;
						}
					} else if (next == null) {
						subSetIf(current, prev);
						prev.next = null;
						current.isValid = false;
						current.lock.unlock();
						return;
					} else {
						prev.next = next;
						subSetIf(current, prev);
						current.isValid = false;
						current.next = null;
						current.lock.unlock();
						current = prev;
						permissionToUnlockPrev = false;
					}
				}

				if (prev != null && permissionToUnlockPrev) {
					prev.lock.unlock();
				}

				prev = current;
				current = current.next;
			}
		} finally {
			if (prev != null) {
				prev.lock.unlock();
			}
		}
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		if (startTime > endTime) {
			throw new IllegalArgumentException("startTime greater than endTime");
		} else if (type == null || type.trim().isEmpty()) {
			throw new IllegalArgumentException("Type cannot be null or empty");
		}
		
		ConcurrentEventIterator current = checkPoint.first();

		Iterator<ConcurrentEventIterator> it = checkPoint.descendingIterator();
		while (it.hasNext()) {
			ConcurrentEventIterator c = it.next();
			if (c.value != null && c.value.timestamp() < startTime) {
				current = c;
				break;
			}
		}
		
		if (current.value == null) {
			return null;
		}
		
		ConcurrentEventIterator newIteratorHead = null;
		ConcurrentEventIterator newIteratorCurrent = null;
		
		while (current != null) {
			ReentrantLock lock = current.lock;
			lock.lock();
			try {
				if (current.value.timestamp() >= startTime && current.value.timestamp() <= endTime && current.value.type().equals(type)) {
					Event event = new Event(current.value.type(), current.value.timestamp());
					ConcurrentEventIterator newIterator = new ConcurrentEventIterator(event);
					
					if (newIteratorHead == null) {
						newIteratorHead = newIterator;
						newIteratorCurrent = newIteratorHead;
					} else {
						newIteratorCurrent.next = newIterator;
						newIteratorCurrent = newIterator;
					}
				} else if (current.value.timestamp() > endTime) {
					break;
				}
				current = current.next;
			} finally {
				lock.unlock();
			}
		}

		return newIteratorHead;
	}

	public int length() {
		return length.get();
	}
	
	private void subSetIf(ConcurrentEventIterator fromElement, ConcurrentEventIterator toElement) {
		if (checkPoint.contains(fromElement)) {
			if (fromElement.value.timestamp() < toElement.value.timestamp()) {
				checkPoint.subSet(fromElement, toElement);
			} else {
				checkPoint.remove(fromElement);
				checkPoint.add(toElement);
			}
		}
	}

	private class EventComparator implements Comparator<ConcurrentEventIterator> {

		@Override
		public int compare(ConcurrentEventIterator o1, ConcurrentEventIterator o2) {
			return (int) (o1.value == null || o2.value == null ? 0 : (o1.value.timestamp() - o2.value.timestamp()));
		}

	}
}
