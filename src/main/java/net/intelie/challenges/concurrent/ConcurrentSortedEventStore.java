package net.intelie.challenges.concurrent;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;
import net.intelie.challenges.EventStore;

/**
 * 
 * <p>
 * A concurrent sorted implementation of the {@link EventStore}
 * 
 * <p>
 * <br>
 * This thread-safe implementation of an ordered linked list stores
 * {@link Event}. {@link EventIterator} allows navigation through events ordered
 * by <i>timestamp</i> inside an {@link Event}. Also, it creates some indexes,
 * named <i>checkPoint</i>, in order to improve performance of the search on a
 * iterator. These checkPoints are created and reorganized automatically, can be
 * limited on the constructor and can be reorganized by the pagination on the
 * constructor.
 * 
 * <p>
 * <br>
 * {@link ConcurrentSkipListSet} stores checkPoints and is ordered by
 * <i>timestamp</i>. The paginations is configurable and works like this: for
 * each ten thousand {@link Event} stored a checkpoint is created. <br>
 * You can limit the number of the checkpoints. If you don't limit, the limit
 * will be the number of elements in this store divided by pagination number did
 * you set or the default number ten thousand.
 * 
 * <p>
 * <br>
 * This class implements a safely concurrent execution of insertion, removal and
 * query operations on {@link Event}. insert operation of an {@link Event} or
 * remove all {@link Event} by type or search for a {@link Event} by type
 * between two timestamps.
 *
 */
public class ConcurrentSortedEventStore implements EventStore {
	private static final int DEFAULT_PAGINATION_CHECKPOINT = 10000;

	private final int paginationCheckPoint;
	private final int maxCheckPoints;

	protected ConcurrentSkipListSet<ConcurrentEventIterator> checkPoint;

	/**
	 * Stores the size of this {@link ConcurrentSortedEventStore}.
	 */
	private AtomicInteger length;

	/**
	 * <p>
	 * This constructor doesn't limit the amount of checkpoints and is based on
	 * pagination's default configuration, which is valued at 10000.
	 */
	public ConcurrentSortedEventStore() {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = DEFAULT_PAGINATION_CHECKPOINT;
		this.maxCheckPoints = 0;
	}

	/**
	 * 
	 * @param paginationCheckPoint express the quantity of elements is allowed on
	 *                             the iterator before creating a new checkpoint.
	 * @param maxCheckPoints       is the maximum number of checkpoints. When valued
	 *                             in zero, the maximum created checkpoint will be
	 *                             the limit number of elements in this store
	 *                             divided by <b>paginationCheckPoint</b>.
	 */
	public ConcurrentSortedEventStore(int paginationCheckPoint, int maxCheckPoints) {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = paginationCheckPoint;
		this.maxCheckPoints = maxCheckPoints;
	}

	/**
	 * <p>
	 * This constructor uses the default max number of pagination (ten thousand)
	 * 
	 * @param maxCheckPoints is the limit of the possible checkpoint will be
	 *                       creates. If this parameter is zero, the max created
	 *                       checkpoint will be the limit will be the number of
	 *                       elements in this store divided by ten thousand
	 *                       (default).
	 */
	public ConcurrentSortedEventStore(int maxCheckPoints) {
		this.length = new AtomicInteger(0);
		this.checkPoint = new ConcurrentSkipListSet<ConcurrentEventIterator>(new EventComparator());
		this.checkPoint.add(new ConcurrentEventIterator());
		this.paginationCheckPoint = DEFAULT_PAGINATION_CHECKPOINT;
		this.maxCheckPoints = maxCheckPoints;
	}

	/**
	 * <p>
	 * Inserts an event into an ordered chain.
	 */
	@Override
	public void insert(Event event) {
		if (event == null) {
			throw new IllegalArgumentException("Event cannot be null");
		} else if (event.type() == null || event.type().trim().isEmpty()) {
			throw new IllegalArgumentException("Type cannot be null or empty");
		}

		// gets the first element of the chain.
		ConcurrentEventIterator current = checkPoint.first();

		// stores checkpoint's position.
		int posCheckPoint = 0;

		// creates a virtual current event iterator to change checkpoint if needed.
		ConcurrentEventIterator virtualCurrent = checkPoint.first();

		// gets the closest checkpoint of the event to be inserted.
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

		// increments chain's size.
		length.incrementAndGet();

		current.lock.lock();

		// this happens when calling removeAll method at the same time of the insertion
		// operation. When removeAll holds the lock of this element of the chain
		// and this current is the same element of the checkpoint it can get an removed
		// element of the chain and this is a invalid element.
		if (!current.isValid) {
			current.lock.unlock();
			insert(event);
			return;
		}

		// when facing an empty chain, current event is null.
		if (current.value == null) {
			current.value = event;
			current.lock.unlock();
			return;
		}

		ConcurrentEventIterator prev = null;

		try {
			long count = 0L;

			// this loop navigates the ordered chain of events to put the new one or just
			// reorganize the checkpoints.
			do {
				// put the event in the ordered chain.
				if (current.value.timestamp() > event.timestamp()) {
					ConcurrentEventIterator aux = new ConcurrentEventIterator(current.next, current.value);
					current.value = event;
					current.next = aux;
					current.lock.unlock();
					return;
				}

				count++;
				// create or reorganize the checkpoints of the chain according to the pagination
				// and the max number of checkpoints.
				long countPos = paginationCheckPoint * posCheckPoint + count;
				if (countPos % paginationCheckPoint == 0) {
					int pos = (int) Math.floorDiv(countPos, paginationCheckPoint);
					if (checkPoint.size() - 1 < pos && maxCheckPoints <= 0
							? checkPoint.size() <= Math.floorDiv(length.get(), paginationCheckPoint)
							: checkPoint.size() <= maxCheckPoints) {
						checkPoint.add(current);
					} else {
						replaceCheckPoint(virtualCurrent, current);
					}
				}

				// on the first insert of an event, previous node value will be null.
				if (prev != null) {
					prev.lock.unlock();
				}

				prev = current;
				if (current.next != null) {
					current.next.lock.lock();
				}
				current = current.next;

			} while (current != null);

			// this code is reached when the event is inserted in the last chain's position.
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
				// verify if the current node is the last on this chain.
				next = current.next;
				if (next != null) {
					next.lock.lock();
					// if this occurs, this chain is empty.
				} else if (current.value == null) {
					current.lock.unlock();
					return;
				}

				boolean permissionToUnlockPrev = true;

				// verifies if this event has the type which needs to be removed.
				if (current.value.type().equals(type)) {
					// decrements chain's size.
					length.decrementAndGet();

					if (prev == null) {
						// this occurs if the chain has only one element.
						if (next == null) {
							current.value = null;
							current.lock.unlock();
							return;
							// this occurs if it is the first element.
						} else {
							current.next = null;
							current.isValid = false;
							replaceCheckPoint(current, next);
							current.lock.unlock();
							current = next;
							continue;
						}
						// this occurs if it is the last element on the chain.
					} else if (next == null) {
						prev.next = null;
						current.isValid = false;
						replaceCheckPoint(current, prev);
						current.lock.unlock();
						return;
					} else {
						prev.next = next;
						current.isValid = false;
						current.next = null;
						replaceCheckPoint(current, prev);
						current.lock.unlock();
						current = prev;
						permissionToUnlockPrev = false;
					}
				}

				// if an element was removed from the middle of the chain, there is no need to
				// unlock the previous element.
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

	/**
	 * Returns a new {@link EventIterator} containing all {@link Event} of the given
	 * {@param type} and <i>timestamp</i> in the range from {@param startTime} to
	 * {@param endTime}.
	 * 
	 * @param type
	 * @param startTime
	 * @param endTime
	 * 
	 */
	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		if (startTime > endTime) {
			throw new IllegalArgumentException("startTime greater than endTime");
		} else if (type == null || type.trim().isEmpty()) {
			throw new IllegalArgumentException("Type cannot be null or empty");
		}

		ConcurrentEventIterator current = checkPoint.first();

		// gets the closest checkpoint of the event to be inserted.
		Iterator<ConcurrentEventIterator> it = checkPoint.descendingIterator();
		while (it.hasNext()) {
			ConcurrentEventIterator c = it.next();
			if (c.value != null && c.value.timestamp() < startTime) {
				current = c;
				break;
			}
		}

		// occurs if the chain is empty.
		if (current.value == null) {
			return null;
		}

		ConcurrentEventIterator newIteratorHead = null;
		ConcurrentEventIterator newIteratorCurrent = null;

		// navigate on all Events and get the result of the query.
		while (current != null) {
			ReentrantLock lock = current.lock;
			lock.lock();
			try {
				if (current.value.timestamp() >= startTime && current.value.timestamp() <= endTime
						&& current.value.type().equals(type)) {
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

	/**
	 * This method replaces the checkpoint value for the new one if the
	 * {@param fromElement} is a checkpoint.
	 * 
	 * @param fromElement the {@link ConcurrentEventIterator} that possible will
	 *                    change.
	 * @param toElement   the {@link ConcurrentEventIterator} the will be the new
	 *                    checkpoint.
	 */
	private void replaceCheckPoint(ConcurrentEventIterator fromElement, ConcurrentEventIterator toElement) {
		if (checkPoint.contains(fromElement)) {
			checkPoint.add(toElement);
			checkPoint.remove(fromElement);
			if (checkPoint.isEmpty()) {
				this.checkPoint.add(new ConcurrentEventIterator());
			}
		}
	}

	/**
	 * This class serves only to sort the ConcurrentSkipListSet.
	 *
	 */
	private class EventComparator implements Comparator<ConcurrentEventIterator> {

		@Override
		public int compare(ConcurrentEventIterator o1, ConcurrentEventIterator o2) {
			return (int) (o1.value == null || o2.value == null ? 0 : (o1.value.timestamp() - o2.value.timestamp()));
		}

	}
}
