package net.intelie.challenges.concurrent;

import java.util.concurrent.locks.ReentrantLock;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class ConcurrentEventIterator implements EventIterator {

	protected ConcurrentEventIterator next;
	protected Event value;
	protected ReentrantLock lock = new ReentrantLock();
	
	public ConcurrentEventIterator() {
	}

	public ConcurrentEventIterator(ConcurrentEventIterator next, Event value) {
		super();
		this.next = next;
		this.value = value;
	}
	
	public ConcurrentEventIterator(Event value) {
		super();
		this.next = null;
		this.value = value;
	}

	@Override
	public void close() throws Exception {
		next = null;
		value = null;
	}

	@Override
	public boolean moveNext() {
		if (next != null) {
			try {
				lock.lock();
				value = next.value;
				next = next.next;
				return true;
			} finally {
				lock.unlock();
			}
		}
		return false;
	}

	@Override
	public Event current() {
		if (value == null) {
			throw new IllegalStateException();
		}
		return value;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "ConcurrentEventIterator [value=" + value + "next=" + next + "]";
	}
}
