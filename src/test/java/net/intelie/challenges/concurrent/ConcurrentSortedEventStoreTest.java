package net.intelie.challenges.concurrent;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class ConcurrentSortedEventStoreTest {
	
	@Test
	public void ordinationAndLengthTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con));
		Thread t5 = new Thread(new MyRunnableInsert(con));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Assert.assertTrue(isSorted(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
	}

	@Test
	public void removeTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread t5 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread tr = new Thread(new MyRunnableRemove(con, "delete"));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Assert.assertEquals(25000, size(con.checkPoint.first()));

		tr.start();
		tr.join();
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(15000, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeAllTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread t2 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread t3 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread t4 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread t5 = new Thread(new MyRunnableInsert(con, "delete"));
		Thread tr = new Thread(new MyRunnableRemove(con, "delete"));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Assert.assertEquals(25000, size(con.checkPoint.first()));

		tr.start();
		tr.join();
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(0, size(con.checkPoint.first()));
		Assert.assertFalse(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeNoOneTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con));
		Thread t2 = new Thread(new MyRunnableInsert(con));
		Thread t3 = new Thread(new MyRunnableInsert(con));
		Thread t4 = new Thread(new MyRunnableInsert(con));
		Thread t5 = new Thread(new MyRunnableInsert(con));
		Thread tr = new Thread(new MyRunnableRemove(con, "delete"));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		Assert.assertEquals(25000, size(con.checkPoint.first()));

		tr.start();
		tr.join();
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeAtFirstTest() {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);
		
		{
			Event event = new Event("delete", 1);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 2);
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
		{
			Event event = new Event("teste", 5);
			con.insert(event);
		}
		
		con.removeAll("delete");
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(3, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeAtLastTest() {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);
		
		{
			Event event = new Event("teste", 1);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 2);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 3);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 6);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 5);
			con.insert(event);
		}
		
		con.removeAll("delete");
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(3, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeAtMiddleTest() {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);
		
		{
			Event event = new Event("teste", 1);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 2);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 3);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 6);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 5);
			con.insert(event);
		}
		
		con.removeAll("delete");
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(3, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void removeAtFirstAtMiddleAtLastTest() {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);
		
		{
			Event event = new Event("delete", 1);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 2);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 3);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 6);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 5);
			con.insert(event);
		}
		{
			Event event = new Event("teste", 8);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 4);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 7);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 10);
			con.insert(event);
		}
		{
			Event event = new Event("delete", 9);
			con.insert(event);
		}
		
		con.removeAll("delete");
		
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
		Assert.assertEquals(2, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}

	@Test
	public void querySizeTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, "query", 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, "query", 20000L));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		EventIterator it = con.query("query", 10000, 16000);

		Assert.assertEquals(5000, size(it));
		
		Assert.assertTrue(isSorted(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
	}

	@Test
	public void querySortedTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, "query", 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, "query", 20000L));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		EventIterator it = con.query("query", 10000, 16000);

		Assert.assertTrue(isSorted(it));
		
		Assert.assertTrue(isSorted(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
	}
	
	@Test
	public void queryNoResultSortedTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, 20000L));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		EventIterator it = con.query("query", 10000, 16000);

		Assert.assertFalse(isSorted(it));
		
		Assert.assertTrue(isSorted(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
	}
	
	@Test
	public void queryNoResultSizeTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, 20000L));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();

		EventIterator it = con.query("query", 10000, 16000);

		Assert.assertEquals(0, size(it));
		
		Assert.assertTrue(isSorted(con.checkPoint.first()));
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertEquals(con.length(), size(con.checkPoint.first()));
	}
	
	@Test
	public void insertAndRemoveTogetherTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, "delete", 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, "delete", 20000L));
		Thread tr = new Thread(new MyRunnableRemove(con, "delete"));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		
		Thread.sleep(500);
		tr.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		tr.join();
		
		Assert.assertNotSame(25000, con.length());
		Assert.assertNotSame(25000, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void insertAndQueryTogetherTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, "query", 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, "query", 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, "query", 20000L));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		
		Thread.sleep(500);
		EventIterator it = con.query("query", 0L, 20000L);

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		
		Assert.assertTrue(isSorted(it));
		Assert.assertEquals(25000, con.length());
		Assert.assertEquals(25000, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	@Test
	public void insertAndRemoveAndQuerySameTogetherTest() throws InterruptedException {
		ConcurrentSortedEventStore con = new ConcurrentSortedEventStore(500);

		Thread t1 = new Thread(new MyRunnableInsert(con, 5000L));
		Thread t2 = new Thread(new MyRunnableInsert(con, 0L));
		Thread t3 = new Thread(new MyRunnableInsert(con, 15000L));
		Thread t4 = new Thread(new MyRunnableInsert(con, "delete", 10000L));
		Thread t5 = new Thread(new MyRunnableInsert(con, "delete", 20000L));
		Thread tr = new Thread(new MyRunnableRemove(con, "delete"));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		
		Thread.sleep(400);
		tr.start();
		
		Thread.sleep(400);
		EventIterator it = con.query("delete", 0L, 20000L);

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		tr.join();
		
		Assert.assertTrue(isSorted(it));
		Assert.assertNotSame(25000, con.length());
		Assert.assertNotSame(25000, size(con.checkPoint.first()));
		Assert.assertTrue(isSorted(con.checkPoint.first()));
	}
	
	private int size(ConcurrentEventIterator current) {
		int count = 0;
		
		if (current.value == null) {
			return count;
		}

		while (current != null) {
			++count;
			current = current.next;
		}

		return count;
	}

	private int size(EventIterator current) {
		int count = 0;

		if (current == null) {
			return 0;
		}

		do {
			++count;
		} while (current.moveNext());

		return count;
	}

	private boolean isSorted(ConcurrentEventIterator current) {
		if (current.value == null) {
			return false;
		}

		while (current != null && current.next != null) {
			if (current.value.timestamp() > current.next.value.timestamp()) {
				return false;
			}
			current = current.next;
		}
		return true;
	}

	private boolean isSorted(EventIterator it) {

		if (it == null || it.current() == null) {
			return false;
		}

		Event prev = it.current();

		do {
			Event current = it.current();
			if (current.timestamp() < prev.timestamp()) {
				return false;
			}
			prev = current;
		} while (it.moveNext());

		return true;
	}
}

class MyRunnableInsert implements Runnable {
	private ConcurrentSortedEventStore con;
	private String type = "teste";
	private Long start = null;

	public MyRunnableInsert(ConcurrentSortedEventStore con) {
		this.con = con;
	}

	public MyRunnableInsert(ConcurrentSortedEventStore con, String type) {
		this.con = con;
		this.type = type;
	}

	public MyRunnableInsert(ConcurrentSortedEventStore con, String type, Long start) {
		this.con = con;
		this.type = type;
		this.start = start;
	}

	public MyRunnableInsert(ConcurrentSortedEventStore con, Long start) {
		this.con = con;
		this.start = start;
	}

	@Override
	public void run() {
		if (start == null) {
			for (int i = 0; i < 5000; i++) {
				Event event = new Event(type, new Random().nextInt(2000000));
				con.insert(event);
			}
		} else {
			for (long i = start.longValue(); i < start.longValue() + 5000; i++) {
				Event event = new Event(type, i);
				con.insert(event);
			}
		}
	}

}

class MyRunnableRemove implements Runnable {
	private ConcurrentSortedEventStore con;
	private String type;

	public MyRunnableRemove(ConcurrentSortedEventStore con, String type) {
		this.con = con;
		this.type = type;
	}

	@Override
	public void run() {
		con.removeAll(type);
	}

}